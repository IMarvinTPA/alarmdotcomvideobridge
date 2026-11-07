from collections import deque

import av
import yaml, sys, json
import typer
import time
import subprocess

from aiortc.rtcrtpreceiver import RemoteStreamTrack, RTCRtpReceiver
from websocketsignaling import WebsocketSignaling
from alarm_janus_orchestrator import AlarmJanusOrchestrator
from pyalarmdotcomajax.models.jsonapi import SuccessDocument

from typing import Any, Optional, Union, cast, List, Dict
import aiortc, aiohttp, asyncio
import pyalarmdotcomajax as adc
from pyalarmdotcomajax.events import ResourceEventMessage
from pyalarmdotcomajax.util import resources_pretty, resources_raw, slug_to_title


from pyalarmdotcomajax.websocket.client import ConnectionEvent, WebSocketState
from pyalarmdotcomajax import AlarmBridge
from pyalarmdotcomajax.adc.util import ValueEnum
from pyalarmdotcomajax.exceptions import (
    AuthenticationFailed,
    MustConfigureMfa,
    NotAuthorized,
    OtpRequired,
    UnexpectedResponse,
)
from pyalarmdotcomajax.models.auth import OtpType


from aiortc import (
    RTCConfiguration,
    RTCIceCandidate,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
    RTCTransportStats,
    VideoStreamTrack,
)
from aiortc.codecs import init_codecs, CODECS
from aiortc.contrib.media import MediaPlayer, MediaRecorder, MediaRecorderContext, MediaStreamTrack, MediaStreamError, AudioFrame, VideoFrame
from aiortc.contrib.signaling import _SignalingObject, BYE
from aiortc.sdp import candidate_from_sdp, candidate_to_sdp

from pprint import pprint

# Setup logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webrtc-rtsp-cli")

# logging.getLogger("aioice").setLevel(logging.DEBUG)
# logging.getLogger("aiortc.rtcdtlstransport").setLevel(logging.DEBUG)
logging.getLogger("aiortc.codecs").setLevel(logging.ERROR)

# Typer CLI initialization
app = typer.Typer(help="Bridge Alarm.com WebRTC signaling source to an RTSP stream.")


def load_config(config_path="config.yaml"):
    # Open the file in read mode
    with open(config_path, "r") as file:
        # safe_load prevents execution of arbitrary code from untrusted files
        config = yaml.safe_load(file)
    return config


class SilenceDecodeFailuresFilter(logging.Filter):
    def filter(self, record):
        # Checks if the log message looks exactly like the one in aiortc
        if "failed to decode" in record.getMessage() and "RTCRtpReceiver" in record.getMessage():
            return False  # Drop the log message completely
        return True  # Allow all other logs through

# 2. Attach the filter to the target aiortc logger
aiortc_receiver_logger = logging.getLogger("aiortc.rtcrtpreceiver")
aiortc_receiver_logger.addFilter(SilenceDecodeFailuresFilter())


class RTSPMediaRecorder:
    """
    An optimized zero-copy media sink that hosts an internal RTP H.264 assembler.
    Asynchronously reads raw RTP fragments from a queue, reconstructs them into
    an Annex-B compliant byte stream, and pushes them down an FFmpeg RTSP pipe.
    Monitors FFmpeg's progress stream to auto-restart on transmission stalls.
    """
    def __init__(self, file: Any, format: Optional[str] = "rtsp", resolution: Optional[str] = None, options: Optional[Dict[str, str]] = None) -> None:
        self.rtsp_url = file
        self.process: Optional[subprocess.Popen] = None
        self.packet_queue = asyncio.Queue(maxsize=300)
        self._worker_task: Optional[asyncio.Task] = None
        self._stdout_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self.fps = 25.0

        self.ffmpeg_log_history = deque(maxlen=100)

        # Tracking state for the health watchdog
        self.last_successful_frame_time = time.time()
        self.is_running = False

        # Stubs for compatibility with your existing monitoring loops
        self.container = self
        self.streams = [type('Stream', (object,), {'frames': 0})()]
        self._fua_buffer = bytearray()

    def addTrack(self, track: Any) -> None:
        if track.kind == "video" and not self.process:
            sprop_sets = ""
            try:
                if hasattr(track, 'track') and hasattr(track.track, '_RemoteStreamTrack__receiver'):
                    pc = track.track._RemoteStreamTrack__receiver._PeerConnection
                    if pc.remoteDescription and pc.remoteDescription.sdp:
                        for line in pc.remoteDescription.sdp.split("\r\n"):
                            if "sprop-parameter-sets=" in line:
                                parts = line.split("sprop-parameter-sets=")
                                if len(parts) > 1:
                                    sprop_sets = parts[1].split(";")[0].strip()
                                    logger.info(f"Extracted dynamic camera parameters: {sprop_sets}")
                                    break
                            elif "a=framerate:" in line.lower():
                                try:
                                    fps_val = float(line.split(":")[1].strip())
                                    self.fps = fps_val
                                    logger.info(f"Dynamic camera stream frame rate discovered: {self.fps} FPS")
                                except (IndexError, ValueError):
                                    pass
            except Exception as sdp_exc:
                logger.debug(f"Could not extract parameter sets from SDP: {sdp_exc}")

            ffmpeg_cmd = [
                "ffmpeg", "-y",
                *(["-sprop-parameter-sets", sprop_sets] if sprop_sets else []),
                "-use_wallclock_as_timestamps", "1",
                "-fflags", "+nobuffer+genpts",
                "-f", "h264",
                "-i", "pipe:0",
                "-progress", "pipe:1",  #Direct FFmpeg to output real-time stats to stdout
                "-c:v", "copy",
                "-flags", "+global_header",
                "-f", "rtsp",
                "-rtsp_transport", "tcp",
                "-muxdelay", "0.1",
                self.rtsp_url
            ]

            logger.info(f"Spawning native transport pipe targeting {self.rtsp_url}...")
            self.process = subprocess.Popen(
                ffmpeg_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, # Required to intercept progress lines
                stderr=subprocess.PIPE   # Captures startup banners and core faults
            )
            self.is_running = True
            self.last_successful_frame_time = time.time()

    async def start(self) -> None:
        """Starts the background asyncio pipe worker and monitoring loops."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self.__pipe_worker_loop())
        if self._stdout_task is None:
            self._stdout_task = asyncio.create_task(self.__ffmpeg_stdout_reader())
        if self._watchdog_task is None:
            self._watchdog_task = asyncio.create_task(self.__watchdog_loop())
        if self._stderr_task is None:
            self._stderr_task = asyncio.create_task(self.__ffmpeg_stderr_drainer())

    async def stop(self) -> None:
        """Gracefully tears down tasks and terminates the background FFmpeg stream."""
        self.is_running = False

        if self._watchdog_task:
            self._watchdog_task.cancel()
            self._watchdog_task = None
        if self._stdout_task:
            self._stdout_task.cancel()
            self._stdout_task = None
        if self._worker_task:
            self._worker_task.cancel()
            self._worker_task = None

        if self.process:
            try:
                if self.process.stdin:
                    self.process.stdin.close()
                self.process.terminate()
                self.process.wait(timeout=2.0)
            except Exception:
                pass
            self.process = None
            logger.info("Zero-copy pipe transport pipeline closed cleanly.")

    def push_packet_safely(self, payload: bytes):
        """Pushes data down into the queue defensively, dropping the oldest packet if full."""
        try:
            self.packet_queue.put_nowait(payload)
        except asyncio.QueueFull:
            try:
                self.packet_queue.get_nowait()
                self.packet_queue.task_done()
                self.packet_queue.put_nowait(payload)
            except Exception:
                pass

    async def __ffmpeg_stdout_reader(self) -> None:
        """Asynchronously reads and parses FFmpeg's progress metrics line by line without buffering."""
        try:
            loop = asyncio.get_running_loop()
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            await loop.connect_read_pipe(lambda: protocol, self.process.stdout)

            # Accumulate string pieces because FFmpeg combines data blocks using '\r' updates
            buffer = ""
            while self.is_running:
                chunk_bytes = await reader.read(1024)
                if not chunk_bytes:
                    break

                buffer += chunk_bytes.decode('utf-8', errors='ignore')
                #logger.info(buffer)
                # Split entries by both carriage returns and newline feeds
                while "\r" in buffer or "\n" in buffer:
                    if "\r" in buffer and ("\n" not in buffer or buffer.index("\r") < buffer.index("\n")):
                        line, buffer = buffer.split("\r", 1)
                    else:
                        line, buffer = buffer.split("\n", 1)

                    line = line.strip()

                    # Intercept the exact frame count values
                    if line.startswith("out_time_us="):
                        # Update our rolling window timestamp anchor instantly
                        self.last_successful_frame_time = time.time()
                        try:
                            # Extract the microsecond value string
                            time_us = int(line.split("=")[1].strip())

                            # Estimate the current frame number (45600000 us = 45.6 seconds)
                            # 45.6 seconds * 25 frames per second = ~1140 frames
                            time_seconds = time_us / 1000000.0
                            self.streams[0].frames = int(time_seconds * self.fps)

                        except (IndexError, ValueError):
                            pass

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"Error in FFmpeg stdout progress reader: {e}")


    async def __ffmpeg_stderr_drainer(self) -> None:
        """Continuously drains FFmpeg's stderr stream asynchronously to prevent deadlocks."""
        try:
            loop = asyncio.get_running_loop()
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            await loop.connect_read_pipe(lambda: protocol, self.process.stderr)

            buffer = ""
            while self.is_running:
                chunk_bytes = await reader.read(1024)
                if not chunk_bytes:
                    break

                buffer += chunk_bytes.decode('utf-8', errors='ignore')
                #logger.info(buffer)
                # Split entries by both carriage returns and newline feeds
                while "\r" in buffer or "\n" in buffer:
                    if "\r" in buffer and ("\n" not in buffer or buffer.index("\r") < buffer.index("\n")):
                        line, buffer = buffer.split("\r", 1)
                    else:
                        line, buffer = buffer.split("\n", 1)

                    line = line.strip()

                    if line:
                        # Append straight to our fixed-size rolling circular deque
                        # if not line.startswith("size="):
                        #     logger.info("STD Error: "  + line)
                        self.ffmpeg_log_history.append(line)

        except asyncio.CancelledError:
            return
        except Exception as e:
            #ValueError: Separator is not found, and chunk exceed the limit
            logger.error(f"Error inside non-blocking stderr drain loop: {str(type(e))} {e}")


    async def __watchdog_loop(self) -> None:
        """Watchdog loop that monitors the age of the last successfully processed frame."""
        try:
            # 5-second silence allowance window threshold
            stall_threshold = 5.0

            while self.is_running:
                await asyncio.sleep(1.0)

                # Check how many seconds have elapsed since FFmpeg last advanced its frame count
                time_since_last_frame = time.time() - self.last_successful_frame_time

                if time_since_last_frame > stall_threshold:
                    logger.warning(f"FFmpeg transmission stall detected! No frame updates for {time_since_last_frame:.1f}s.")
                    logger.warning("Initiating custom restart pipeline sequence...")

                    # Read stderr to preserve log traces before wiping the context
                    if self.ffmpeg_log_history:
                        logger.error("--- FFmpeg Console Error Output Captured From Memory ---")
                        for historic_line in self.ffmpeg_log_history:
                            logger.error(historic_line)
                        logger.error("---------------------------------------------------------")

                    # Raise a pipeline exception to be caught or bubble out directly
                    # Alternatively, call a local hook to notify your main loop to cycle recorder instances
                    raise RuntimeError("FFmpegMuxingStall")

        except asyncio.CancelledError:
            return

    async def __pipe_worker_loop(self) -> None:
        """
        Asynchronously pulls raw wire bytes out of the queue, parses H.264 NAL units,
        appends Annex-B start codes, and pushes them down the pipe.
        """
        try:
            start_code = b"\x00\x00\x00\x01"
            while True:
                # logger.info("packet queue pops size: " + str(self.packet_queue.qsize()))
                rtp_payload = await self.packet_queue.get()
                # logger.info("packet queue popt size: " + str(self.packet_queue.qsize()))
                if not rtp_payload or len(rtp_payload) < 2:
                    self.packet_queue.task_done()
                    continue



                # Parse the first byte containing the NAL unit type rules
                nal_header = rtp_payload[0]
                nal_type = nal_header & 0x1F

                out_bytes = bytearray()

                # Case 1: Single NAL Unit (Types 1-23)
                if 1 <= nal_type <= 23:
                    out_bytes.extend(start_code)
                    out_bytes.extend(rtp_payload)

                # Case 2: STAP-A Aggregation Packet (Type 24)
                elif nal_type == 24:
                    pos = 1
                    while pos < len(rtp_payload) - 2:
                        # Extract the 2-byte size header for the nested unit
                        nal_size = (rtp_payload[pos] << 8) | rtp_payload[pos + 1]
                        pos += 2
                        if pos + nal_size <= len(rtp_payload):
                            out_bytes.extend(start_code)
                            out_bytes.extend(rtp_payload[pos : pos + nal_size])
                        pos += nal_size

                # Case 3: FU-A Fragmentation Unit (Type 28)
                elif nal_type == 28:
                    fu_header = rtp_payload[1]
                    start_bit = (fu_header & 0x80) >> 7
                    end_bit = (fu_header & 0x40) >> 6
                    inner_type = fu_header & 0x1F

                    # Reconstruction byte structure reconstruction
                    if start_bit == 1:
                        # Rebuild original standard H.264 NAL header prefix
                        rebuilt_header = (nal_header & 0xE0) | inner_type
                        self._fua_buffer = bytearray([rebuilt_header])
                        self._fua_buffer.extend(rtp_payload[2:])
                    else:
                        # Append middle or ending fragments to active memory stack
                        self._fua_buffer.extend(rtp_payload[2:])

                    if end_bit == 1:
                        out_bytes.extend(start_code)
                        out_bytes.extend(self._fua_buffer)
                        self._fua_buffer = bytearray()

                # Commit assembled Annex-B frames immediately to FFmpeg stdin
                if out_bytes and self.process and self.process.stdin:
                    try:
                        self.process.stdin.write(out_bytes)
                        self.process.stdin.flush()
                    except BrokenPipeError:
                        logger.error("FFmpeg pipe closed unexpectedly during transaction.")
                        if self.process.stderr:
                            errors = self.process.stderr.read().decode('utf-8', errors='ignore')
                            logger.error("--- FFmpeg Console Error Output ---")
                            logger.error(errors)
                            logger.error("-----------------------------------")
                        return
                self.packet_queue.task_done()
                # logger.info("packet queue pope size: " + str(self.packet_queue.qsize()))
        except asyncio.CancelledError:
            return

class AlarmVideoRelay:
    ffmpeg_process: subprocess.Popen = None
    wss: WebsocketSignaling = None
    alarmBridge: AlarmBridge = None
    signalling_server_url = ""
    signalling_server_token = ""
    camera_auth_token = ""
    resolution = "800x600"
    src_pc: RTCPeerConnection = None
    dst_pc: RTCPeerConnection = None
    recorder: MediaRecorder = None
    recorder_turned_on: bool = False
    seenTracks: List[MediaStreamTrack] = None
    janus_gateway_url = ""
    janus_token = ""
    ice_servers = None
    proxy_url = ""


    requestedCameraId = ""
    cameraMacAddress = ""
    cameraModel = ""
    cameraFirmwareVersion = ""
    cameraNotDialedIn = False

    def __init__(self):
        config_path="config.yaml"
        self.config = {}
        with open(config_path, "r") as file:
            # safe_load prevents execution of arbitrary code from untrusted files
            self.config = yaml.safe_load(file)

        logger.info(self.config)

        self.rtsp_output_url = "rtsp://" + self.config["service_endpoint"]["host"] + ":" + str(self.config["service_endpoint"]["port"]) + "/" + self.config["service_endpoint"]["path"]
        logger.info("Streaming to " + self.rtsp_output_url)

        self.seenTracks = []
        self.ice_servers = []
        self.cameraNotDialedIn = False


    async def connect(self, username: str,
                      password: str,
                      cookie: str,
                      ):

        #####################
        # INITIALIZE BRIDGE #
        #####################

        self.alarmBridge = AlarmBridge()

        self.alarmBridge.auth_controller.set_credentials(username, password, cookie)

        # Typer calls bridge.close() when context is closed.
        #ctx.call_on_close(partial(asyncio.get_event_loop().run_until_complete, bridge.close()))

        #
        # LOG IN
        #

        try:
            # Initialize the connector
            await self.alarmBridge.initialize()

        except MustConfigureMfa as err:
            logger.error("[red]Unable to log in. Please set up two-factor authentication for this account.")
            raise

        except (TimeoutError, aiohttp.ClientError, UnexpectedResponse, NotAuthorized) as err:
            logger.error("[red]Could not connect to Alarm.com.")
            raise

        except AuthenticationFailed as err:
            logger.error("[red]Invalid credentials.")
            raise

        except OtpRequired as exc:
            logger.error("[bold red]OTP Required, use the adp cli tool to get your cookie.")
            raise

    async def validateCamera(self) -> bool:
        description = self.config["camera"]["description"]
        video_source = self.config["camera"]["video_source"]
        stream_id = self.config["camera"]["stream_id"]

        #TODO: Validate values exist.

        logger.info("Retrieving cameras.")

        resp: SuccessDocument = await self.alarmBridge.request("GET", "https://www.alarm.com/web/api/video/devices/cameras") #?filterByLiveViewDisplayable=true

        logger.debug(resp)

        raw_cameras = resp.to_dict()["data"]
        cameras = {}
        requestedCameraId = None
        self.requestedCameraId = ""
        self.cameraMacAddress = ""
        self.cameraModel = ""
        self.cameraFirmwareVersion = ""

        useHighest = True

        for raw_camera in raw_cameras:
            logger.info("Camera found: " + raw_camera["id"])
            logger.info("description: " + raw_camera["attributes"]["description"])

            # for rel in raw_camera["relationships"]:
            #     print(rel, raw_camera["relationships"][rel])
            # for rel in raw_camera["attributes"]:
            #     print(rel, raw_camera["attributes"][rel])

            logger.info("video_source: " + raw_camera["relationships"]["videoSource"]["data"]["type"])
            logger.info("video_source_hd: " + raw_camera["relationships"]["videoSourceHd"]["data"]["type"])
            logger.debug(raw_camera)
            id = raw_camera["id"]
            camera = {"id": id, "raw": raw_camera}
            camera["name"] = raw_camera["attributes"]["description"]
            camera["sd"] = raw_camera["relationships"]["videoSource"]["data"]["type"]
            camera["hd"] = raw_camera["relationships"]["videoSourceHd"]["data"]["type"]
            cameras[id] = camera
            if camera["name"] == description:
                requestedCameraId = id
                self.requestedCameraId = id
                self.cameraMacAddress = raw_camera["attributes"]["macAddress"]
                self.cameraModel = raw_camera["attributes"]["deviceModel"]
                self.cameraFirmwareVersion = raw_camera["attributes"]["firmwareVersion"]
                if camera["sd"].endswith("/" + video_source):
                    video_source = camera["sd"]
                    useHighest = False
                if camera["hd"].endswith("/" + video_source):
                    video_source = camera["hd"]
                    useHighest = True

        if not requestedCameraId:
            logger.info("No camera of the requested name found.")
            return False

        if useHighest:
            video_source = "video/videoSources/liveVideoHighestResSources"
        else:
            video_source = "video/videoSources/liveVideoSources"

        #This gets our list of cameras and stuff:
        resp: SuccessDocument = await self.alarmBridge.request("GET", "https://www.alarm.com/web/api/" + video_source + "/" + requestedCameraId)

        logger.debug(resp)
        #print(resp)
        #pprint(resp)

        self.signalling_server_url = ""
        self.signalling_server_token = ""
        self.camera_auth_token = ""
        self.janus_gateway_url = ""
        self.janus_token = ""
        self.ice_servers = []
        self.proxy_url = ""

        self.resolution = "800x600"

        dataAttributes = resp.data.attributes
        self.janus_gateway_url = dataAttributes.get("janus_gateway_url")
        self.janus_token = dataAttributes.get("janus_token")
        self.proxy_url = dataAttributes.get("proxy_url")


        for r in resp.included:
            logger.info(r.type)
            if r.type == "video/videoSources/endToEndWebrtcConnectionInfo":
                logger.info(r.attributes)
                for a in r.attributes:
                    logger.info(a + ": " + str(r.attributes[a]))

                # Parse ICE servers JSON string if returned as string
                if "ice_servers" in r.attributes:
                    if isinstance(r.attributes["ice_servers"], str):
                        self.ice_servers = json.loads(r.attributes["ice_servers"])
                    else:
                        self.ice_servers = r.attributes["ice_servers"]

                self.signalling_server_url = r.attributes["signalling_server_url"]
                self.signalling_server_token = r.attributes["signalling_server_token"]
                self.camera_auth_token = r.attributes["camera_auth_token"]
            elif r.type == "video/videoSources/webrtcStreamQualityMessage":
                if r.attributes["stream_id"] == stream_id:
                    self.resolution = r.attributes["resolution"]
                    print("Using the camera at " + camera["name"] + " (" + requestedCameraId + ") with a resolution of " + r.attributes["resolution"] + ":")
                else:
                    print("To use the camera at " + camera["name"] + " (" + requestedCameraId + ") with a resolution of " + r.attributes["resolution"] + " use the below in the yaml:")
                print("camera:")
                print("  description: " + camera["name"])
                if useHighest:
                    print("  video_source: liveVideoHighestResSource")
                else:
                    print("  video_source: liveVideoSource")
                print("  stream_id: " + str(r.attributes["stream_id"]))

        logger.info("signalling_server_url: " + self.signalling_server_url)
        logger.info("signalling_server_token: " + self.signalling_server_token)
        logger.info("camera_auth_token: " + self.camera_auth_token)
        logger.info("janus_gateway_url: " + self.janus_gateway_url)
        logger.info("janus_token: " + self.janus_token)
        logger.info("proxy_url: " + self.proxy_url)
        logger.info("ice_servers: " + str(self.ice_servers))

        return True

        # await asyncio.sleep(7)

        #https://www.alarm.com/web/api/video/devices/cameras/99580595-2048/restartWebrtc
        #This seems to be the trigger to make the camera need to dialin.
        # try:
        #     resp: SuccessDocument = await self.alarmBridge.request("POST", "https://www.alarm.com/web/api/video/devices/cameras/" + requestedCameraId + "/restartWebrtc")

        #     logger.info("Restarting WebRTC command response:")
        #     logger.info(resp)
        # except Exception as e:
        #     logger.error(f"WebRTC command returns empty result which request doesn't like.: {e}")

        # await asyncio.sleep(7)
        #TODO:  Make this list out suggestions for


    async def establishFeed(self) -> bool:
        """
        Create or recreate a new Alarm Bridge and find our camera's current feed.
        """
        username = self.config["alarm_auth"]["alarm_user"]
        password = self.config["alarm_auth"]["alarm_password"]
        cookie = self.config["alarm_auth"]["alarm_cookie"]

        #TODO: Validate values exist.eparesookie)

        logger.info("Attempting to connect to Alarm.com as " + username)
        await self.connect(username, password, cookie)

        ili = await self.alarmBridge.is_logged_in()
        logger.info("Alarm.com connection successful.")

        if not await self.validateCamera():
            return False

        logger.info("Camera process completed.")
        return True


    async def start_recorder_safely(self):
        """Injects direct network transport extraction hooks seamlessly without thread blocks."""
        try:
            if self.recorder:
                logger.info("Spawning production-optimized zero-copy transport loop...")
                await self.recorder.start()

                # ─── THE DIRECT NETWORK SOCKET MONKEY-PATCH ───
                # Reach straight down into the connection's active DTLS transport mechanism layer.
                # This grabs raw RTP payload chunks immediately as they pass through the network wire.
                for transceiver in self.src_pc.getTransceivers():
                    if transceiver.kind == "video" and transceiver.receiver.transport:
                        dtls_transport = transceiver.receiver.transport
                        target_queue = self.recorder.packet_queue

                        #Save off the original handle in case we have to restart our stream.
                        if not hasattr(dtls_transport, "original_handle_rtp_data"):
                            dtls_transport.original_handle_rtp_data = dtls_transport._handle_rtp_data

                        # Reference original data consumer hook
                        original_handle_rtp_data = dtls_transport.original_handle_rtp_data

                        # Create inline wrapper to intercept raw network traffic seamlessly
                        async def zero_copy_rtp_interceptor(data, arrival_time_ms=None):
                            # Ensure we are handling structured RTP media packets rather than STUN/RTCP noise
                            if len(data) > 12 and (data[0] & 0xC0) == 0x80:
                                # Stripping standard 12-byte RTP fixed layout boundaries to expose H.264 bits
                                #logger.info("Intercepting useful packets.")
                                rtp_payload = data[12:]
                                target_queue.put_nowait(rtp_payload)
                                # logger.info("packet queue push size: " + str(target_queue.qsize()))
                                ghost_packet = data[:12]
                                await original_handle_rtp_data(ghost_packet, arrival_time_ms=arrival_time_ms)
                            else:
                                # Pass non-video data (like audio or STUN) through normally
                                await original_handle_rtp_data(data, arrival_time_ms=arrival_time_ms)

                        # Overlay the patch definition onto live environment instance structures
                        dtls_transport._handle_rtp_data = zero_copy_rtp_interceptor
                        logger.info("Successfully bound zero-copy stream loop directly to network socket layer.")

                self.recorder_turned_on = True
                print("Turning Recorder on")
        except Exception as e:
            logger.error(f"Failed to cleanly start media streaming container: {e}")

    def _sync_recorder_worker(self):
        # This executes safely isolated in its own thread block
        asyncio.run(self.recorder.start())
        self.recorder_turned_on = True
        print("Turning Recorder on")

    async def createRecorder(self, tracks: List[RemoteStreamTrack]):
        #Instantiate MediaRecorder with your RTSP path and specific ffmpeg parameters
        # -f rtsp forces the rtsp container format output type
        if tracks:
            logger.info("Recreating Recorder.")
        else:
            logger.info("Creating Recorder.")

        self.recorder = RTSPMediaRecorder(
            self.rtsp_output_url,
            format="rtsp",
            resolution=self.resolution,
            options={
                # # 1. Enforce interleaved TCP for streaming output
                "rtsp_transport": "tcp",

                # # 2. Eliminate buffering lag
                "muxdelay": "0.1",

                # # 3. Clean space-separated output flags (NO '+' signs)
                # #    nobuffer: minimize caching
                # #    flush_packets: write out immediately
                # #    global_header: handles camera parameter changes mid-stream
                # #    igndts: ignores invalid decoding timestamp spikes
                # #    genpts: generates missing timestamps
                "fflags": "+nobuffer+flush_packets+global_header+igndts+genpts",

                # 4. Force the H.264 auto bitstream filter for raw WebRTC frames
                #"auto_bsf": "1"
            }
        )
        for track in tracks:
            self.recorder.addTrack(track)

    async def _monitor_recorder_health(self):
        print("Recorder exception watchdog started...")
        try:
            # Access the private list of background writer loops via name-mangling
            recorder_tasks = self.recorder._RTSPMediaRecorder__tasks
        except AttributeError:
            print("Failed to bind to internal RTSPMediaRecorder tasks layout.")
            return

        while True:
            await asyncio.sleep(0.5)  # Quick, low-overhead poll rate

            for task in recorder_tasks:
                # Check if the internal __run_track loop has completed/crashed
                if task.done():
                    try:
                        #This extracts and retrieves the hidden exception!
                        exc = task.exception()
                        if exc:
                            print(f"\n[CRASH DETECTED] MediaRecorder background task raised: {repr(exc)}")

                            # Handle specific failures or trigger a full rebuild sequence
                            if isinstance(exc, ValueError) and "22" in str(exc):
                                print("Error matches standard RTSP timeline/parameter mutation fault.")

                            # Stop the dead recorder context cleanly
                            await self.recorder.stop()

                            #Call your application restart method here
                            #self.trigger_pipeline_restart()
                            await self.createRecorder(self.seenTracks)
                            await self.start_recorder_safely()
                            return

                    except asyncio.CancelledError:
                        pass  # Task was shut down intentionally by a normal stop() call

    async def createPeerConnection(self, configuration: RTCConfiguration = None) -> RTCPeerConnection:
        self.src_pc = RTCPeerConnection(configuration)
        # We must add transceivers to tell the remote source we are ready to RECEIVE media
        # video_transceiver = self.src_pc.addTransceiver("video", direction="recvonly")
        # self.src_pc.addTransceiver("audio", direction="recvonly")
        # try:
        #     if video_transceiver.receiver:
        #         # Force the capabilities map to contain empty rules so decoding goes dormant
        #         video_transceiver.receiver._codecs = []
        #         logger.info("Successfully disabled internal aiortc H.264 decoding loops.")
        # except Exception as codec_exc:
        #     logger.debug(f"Could not drop receiver codec registration profiles: {codec_exc}")

        return self.src_pc

    async def manageFeed(self):
        """
        Connects to Alarm.com, negotiates the WebRTC connection,
        and saves tracks to be consumed by the WHIP endpoint.
        """

        #Clean up any existing incoming requests.
        if self.src_pc:
            await self.src_pc.close()


        #Mostly just to make the frame monitoring loop have an object.
        await self.createPeerConnection()

        await self.createRecorder(self.seenTracks)

        remote_description_set = False
        replayConf = []
        replayDesc = []
        replayICE = []
        orchestrator = None

        alarm_ws_url = f"{self.signalling_server_url}/{self.signalling_server_token}"
        janus_ws_url = self.janus_gateway_url # e.g., wss://://devicetask.com


        async with self.alarmBridge.ws_connect(alarm_ws_url) as webSockConn:
            alarm_signaling: WebsocketSignaling = WebsocketSignaling(webSockConn)

            async def manageAlarmMessage(obj: Any,
                                    ) -> bool:
                """
                Process an alarm message.

                Args:
                    obj (Any): Something the Alarm WebRTC web service sends.

                Returns:
                    bool: if True, then we need to terminate the loop
                """
                # Route inbound message blocks dynamically
                if isinstance(obj, RTCConfiguration):
                    if not obj.iceServers:
                        from aiortc import RTCIceServer
                        obj.iceServers = [
                            RTCIceServer(urls="stun:://google.com"),
                            RTCIceServer(urls="stun:://google.com")
                        ]
                    if orchestrator:
                        await update_orchestrator_pc(obj)
                    else:
                        await self.createPeerConnection(obj)
                        self.src_pc.add_listener("track", on_track)
                        self.src_pc.add_listener("connectionstatechange", on_connectionstatechange)

                    self.src_pc.add_listener("icecandidate", on_icecandidate)

                    replayICE.clear()
                    self.seenTracks.clear()

                    if self.recorder and self.recorder_turned_on:
                        await self.recorder.close()
                    await self.createRecorder(self.seenTracks)

                elif isinstance(obj, RTCSessionDescription):
                    logger.info(f"Applying remote session description update: {obj.type}")
                    await self.src_pc.setRemoteDescription(obj)
                    for queued_cand in replayICE:
                        logger.info(f"Using queued ice candidate: {queued_cand}")
                        await self.src_pc.addIceCandidate(queued_cand)
                    if obj.type == "offer":
                        await self.src_pc.setLocalDescription(await self.src_pc.createAnswer())
                        await alarm_signaling.send(self.src_pc.localDescription)

                elif isinstance(obj, RTCIceCandidate):
                    logger.info("Got an ICE Candidate.")
                    if self.src_pc and self.src_pc.remoteDescription:
                        await self.src_pc.addIceCandidate(obj)
                    else:
                        replayICE.append(obj)
                elif "error" in obj:
                    if "has not yet dialed in" in str(obj.get("error", "")):
                        logger.info("Camera has not yet dialed in, we need to wait for janus.")
                        self.cameraNotDialedIn = True
                        return False
                    logger.info(obj)
                    logger.info("Do something here to terminate the whole feed.")
                    return True
                else:
                    logger.info(obj)

                return False

            async def frameMonitoringLoop(ws_object):
                prevFrames = -1
                prevPackets = -1
                seenSameFramesCnt = 0
                seenConnectingCnt = 0
                seenSamePacketsCnt = 0
                logger.info("Starting Monitoring Loop.")
                while self.src_pc.connectionState in("connected", "connecting", "new") and not ws_object.closed:
                    logger.debug("pc.connectionState: " + self.src_pc.connectionState)
                    await asyncio.sleep(2)

                    if seenConnectingCnt > 3:
                        logger.warning("Connection Frozen, aborting.")
                        return
                    elif seenConnectingCnt > 0:
                        logger.warning("Connection cooling. " + str(seenConnectingCnt))

                    if seenSamePacketsCnt > 3:
                        logger.warning("Packets Frozen, aborting.")
                        return
                    elif seenSamePacketsCnt > 0:
                        logger.warning("Packets cooling. " + str(seenSamePacketsCnt))

                    if self.src_pc.connectionState in("connecting", "new"):
                        seenConnectingCnt += 1
                    else:
                        seenConnectingCnt = 0

                    if self.recorder_turned_on: # self.recorder and hasattr(self.recorder, "_MediaRecorder__container"):
                        # Access the private PyAV/FFmpeg container object safely
                        container = self.recorder.container

                        # Example: Check if the container is open and active
                        if container is not None:
                            #print(dir(container))

                            # logger.info(f"Container Dump: {container}")
                            # logger.info(f"Container Format: {container.format.name}")
                            # logger.info(f"Container Streams: {len(container.streams)}")
                            # logger.info(f"Stream disposition: {container.streams[0].disposition}")
                            # logger.info(f"Stream duration: {container.streams[0].duration}")

                            logger.debug(f"Outgoing Stream Frame: {container.streams[0].frames}")

                            if container.streams[0].frames == prevFrames:
                                logger.warning("Seen this frame count " + str( seenSameFramesCnt + 1) + " times.")
                                if seenSameFramesCnt > 2:
                                    logger.warning("Frozen frame threshold reached. Forcing recorder restart cycle...")
                                    seenSameFramesCnt = 0
                                    prevFrames = -1

                                    # print("Turning Recorder off")
                                    # await self.recorder.stop()
                                    await self.createRecorder(self.seenTracks)
                                    await self.start_recorder_safely()
                                else:
                                    seenSameFramesCnt += 1
                            else:
                                prevFrames = container.streams[0].frames
                                seenSameFramesCnt = 0

                            #print(container.streams[0])
                            #print(dir(container.streams[0]))
                        else:
                            logger.warning("Container has not been initialized yet or has been closed.")

                    stats = await self.src_pc.getStats()

                    for report_id, report in stats.items():
                        # rep: RTCTransportStats = report
                        # rep.packetsReceived
                        if report_id.startswith("inbound-rtp"):
                            #print(type(report))
                            #print("report id:", report_id, report)
                            packets = report.packetsReceived
                            logger.debug("     Packets Received: " + str(packets))
                            if prevPackets == packets:
                                seenSamePacketsCnt +=1
                            else:
                                seenSamePacketsCnt = 0
                            prevPackets = packets

                    if self.src_pc.connectionState == "connected":
                        logger.debug("iceConnectionState: " + self.src_pc.iceConnectionState)
                        logger.debug("signalingState: " + self.src_pc.signalingState)

            async def alarm_signaling_drainer():
                logger.info("Starting background consumer task for Alarm Bridge control plane...")
                try:
                    while not webSockConn.closed:
                        obj = await alarm_signaling.receive()
                        if obj is None:
                            await asyncio.sleep(0.1)
                            continue
                        if obj is BYE:
                            logger.info("Exiting Alarm Bridge signaling consumer context cleanly.")
                            break

                        if await manageAlarmMessage(obj):
                            break

                except asyncio.CancelledError:
                    return
                except Exception as drain_ex:
                    logger.error(f"Exception caught inside background control drain loop: {drain_ex}")


            def on_track(track: RemoteStreamTrack):
                logger.info(f"Proxy successfully received external track: {track.kind}")
                print(type(track))
                print(track)

                #track = MonotonicTimestampProxy(track)

                # Feed the track directly into the ffmpeg-backed recorder container
                self.recorder.addTrack(track)
                self.seenTracks.append(track)

            async def on_connectionstatechange():
                if self.src_pc is None:
                    return
                logger.info(f"External Source connection state: {self.src_pc.connectionState}")
                logger.info(self.src_pc)
                logger.info(alarm_signaling)
                if self.src_pc.connectionState == "connected":
                    logger.info("WebRTC connection established successfully")
                    stream = {"streamID": self.config["camera"]["stream_id"],"resolution": self.resolution}
                    if not self.cameraNotDialedIn:
                        await alarm_signaling.sendCommand("CHANGE_STREAM", "stream", stream)
                    await self.start_recorder_safely()
                elif self.src_pc.connectionState in ["failed", "closed"]:
                    logger.info("WebRTC connection terminated")
                    #await self.src_pc.close()
                    #self.src_pc = None
                    if self.recorder:
                        logger.info("Turning Recorder off")
                    await self.recorder.stop()
                    self.recorder = None
                    self.recorder_turned_on = False

            async def on_icecandidate(candidate):
                if candidate:
                    logger.info(f"Generated local ICE Candidate: {candidate}")
                    # Forward your local candidate to Alarm.com so it knows where to reply
                    await alarm_signaling.send(candidate)
                else:
                    logger.info("Local ICE candidate gathering completely finished.")



            logger.info("Sending Start Session command.")

            self.cameraNotDialedIn = False

            await webSockConn.send_str("START_SESSION " + self.camera_auth_token)

            logger.info("Sent Start Session command.  Waiting for response.")

            # 2. Check the initial message payload response dropping down the pipe
            initial_obj = await alarm_signaling.receive()

            logger.info("Got a response: " + str(initial_obj))

            await manageAlarmMessage(initial_obj)

            logger.info("Processed initial response.")
            logger.info("self.cameraNotDialedIn: " + str(self.cameraNotDialedIn))

            if self.cameraNotDialedIn:
                #https://www.alarm.com/web/api/video/devices/cameras/99580595-2048/restartWebrtc
                #This seems to be the trigger to make the camera need to dialin.
                #This is also needed to START the dial-in process once it is not dialed in.
                try:
                    logger.info("Restarting WebRTC")
                    resp: SuccessDocument = await self.alarmBridge.request("POST", "https://www.alarm.com/web/api/video/devices/cameras/" + self.requestedCameraId + "/restartWebrtc")

                    logger.info("Restarting WebRTC command response:")
                    logger.info(resp)
                except Exception as e:
                    logger.error(f"WebRTC command returns empty result which request doesn't like.: {e}")

                await asyncio.sleep(7)


                logger.info("Connecting to janus")
                async with aiohttp.ClientSession() as session:
                    logger.info("janus_ws_url: " + janus_ws_url)
                    async with session.ws_connect(janus_ws_url,
                                                protocols=["janus-protocol"],
                                                ) as janus_ws:
                        logger.info("Connected to janus")
                        orchestrator = AlarmJanusOrchestrator(
                            custom_signal=janus_ws,  # Standard aiohttp ClientWebSocketResponse
                            janus_token=self.janus_token,
                            proxy_url=self.proxy_url,
                            resolution=self.resolution,
                            requestedCameraId=self.requestedCameraId, # Match tracking opaque_id
                            cameraMacAddress=self.cameraMacAddress, # Captured from Sent 3 hardware strings profiles
                            cameraModel=self.cameraModel,
                            cameraFirmwareVersion=self.cameraFirmwareVersion
                        )

                        async def update_orchestrator_pc(configuration: RTCConfiguration):
                            logger.info("Hot-swapping active RTCPeerConnection due to incoming RTCConfiguration update...")

                            # Stop existing context cleanly to avoid memory leaks or locked sockets
                            if orchestrator.src_pc:
                                try:
                                    await orchestrator.src_pc.close()
                                except Exception:
                                    pass

                            # Instantiate the replacement peer connection inside the orchestrator using the fresh ICE profiles
                            self.src_pc = await orchestrator.initialize_peer_connection(
                                on_track_cb=on_track,
                                on_state_change_cb=on_connectionstatechange,
                                configuration=configuration,
                            )
                            self.src_pc.add_listener("icecandidate", on_icecandidate)

                            logger.info("Successfully updated orchestrator with the new optimized RTCPeerConnection wrapper.")

                        janus_ice_config = RTCConfiguration()
                        if self.ice_servers:
                            logger.info("Building explicit WAN signaling route using Alarm.com ICE configurations...")
                            for ice_entry in self.ice_servers:
                                urls = ice_entry.get("urls", [])
                                if isinstance(urls, str):
                                    urls = [urls]

                                server_obj = RTCIceServer(urls=urls)
                                if "username" in ice_entry:
                                    server_obj.username = ice_entry["username"]
                                if "credential" in ice_entry:
                                    server_obj.credential = ice_entry["credential"]
                                janus_ice_config.iceServers.append(server_obj)
                        else:
                            # Robust fallback routing markers if API entries return vacant parameters mapping arrays
                            logger.warning("No custom ICE profiles returned by the video API. Binding public default targets...")
                            janus_ice_config.iceServers = [
                                RTCIceServer(urls="stun:://google.com"),
                                RTCIceServer(urls="stun:://google.com")
                            ]

                        logger.info(str(janus_ice_config))
                        self.src_pc = await orchestrator.initialize_peer_connection(
                            on_track_cb=on_track,
                            on_state_change_cb=on_connectionstatechange,
                            configuration=janus_ice_config,
                        )
                        self.src_pc.add_listener("icecandidate", on_icecandidate)

                        # Spawn consumer loop task asynchronously
                        #I don't think this will work when dial-in is failed.
                        #alarm_drain_task = asyncio.create_task(alarm_signaling_drainer())


                        logger.info("Starting Orchestrator Pipeline.")
                        await orchestrator.start_pipeline()

                        try:
                            logger.info("Starting Orchestrator Frame Monitoring Loop.")
                            await frameMonitoringLoop(janus_ws)
                        finally:
                            logger.info("De-initializing custom video relay pipeline frameworks...")
                            #alarm_drain_task.cancel()
                            # try:
                            #     await alarm_drain_task
                            # except asyncio.CancelledError:
                            #     pass
                            await orchestrator.tear_down_pipeline()

            else:
                    # Spawn consumer loop task asynchronously
                    alarm_drain_task = asyncio.create_task(alarm_signaling_drainer())
                    try:
                        await frameMonitoringLoop(webSockConn)
                    finally:
                        logger.info("De-initializing custom video relay pipeline frameworks...")
                        alarm_drain_task.cancel()
                        try:
                            await alarm_drain_task
                        except asyncio.CancelledError:
                            pass

        logger.info("Alarm.com stream ingestion complete. Terminating.")
        return True


    async def run(self) -> bool:
        """
        Supervises the persistent loop pipeline. Automatically recovers from network dropouts,
        session token expirations, and WebSocket failures using an exponential backoff strategy.
        """
        logger.info("Starting Persistent Pipeline Supervisor Engine.")
        backoff = 10.0

        while True:
            try:
                # 1. Establish/Refresh auth sessions and validate the active camera profile tokens
                if not await self.establishFeed():
                    return False
                    #raise RuntimeError("EstablishFeedFailed")

                # Reset backoff window upon successful API session validation
                backoff = 5.0

                # 2. Drive the signaling message exchange loop
                # If the camera times out or drops, this unblocks or throws an exception
                await self.manageFeed()

                logger.info("Pipeline cycled natively. Initializing dynamic recovery reconnect...")

            except (AuthenticationFailed, MustConfigureMfa, NotAuthorized, OtpRequired) as fatal_err:
                # Clear execution paths immediately on structural security alterations
                logger.critical(f"[FATAL AUTH ERROR] Stream recovery aborted: {fatal_err}")
                break

            except (asyncio.TimeoutError, aiohttp.ClientError, UnexpectedResponse, Exception) as recoverable_err:
                logger.error(f"[RECOVERABLE STREAM FAULT] Connection broken: {repr(recoverable_err)}")
                logger.info(f"Retrying whole pipeline authentication sequence in {backoff:.1f} seconds...")

                # Perform localized state mitigation to avoid leaving phantom sockets open
                await self.cleanup()

                await asyncio.sleep(backoff)
                # Increment backoff exponentially up to a maximum 60-second limit
                backoff = min(backoff * 1.5, 60.0)

        return True

    async def cleanup(self):
        logger.info("Performing pipeline cleanup operations...")
        if self.src_pc:
            try:
                await self.src_pc.close()
            except Exception:
                pass
            self.src_pc = None
        # if self.dst_pc:
        #     await self.dst_pc.close()
        if self.alarmBridge:
            try:
                await self.alarmBridge.close()
                logger.info("Alarm Bridge closed.")
            except Exception:
                pass
            self.alarmBridge = None
        if self.recorder:
            print("Turning Recorder off")
            await self.recorder.stop()  # Gracefully shutdown ffmpeg process
            self.recorder_turned_on = False
            self.recorder = None
        #sys.exit(0)




@app.command()
def start():
    relay = AlarmVideoRelay()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run the primary thread execution inside our explicit loop handle
        loop.run_until_complete(relay.run())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user keyboard.")
    finally:
        # before closing the thread engine permanently.
        logger.info("Closing active communication frameworks...")
        loop.run_until_complete(relay.cleanup())

        # Cancel any remaining loose background tracking tasks safely
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()

        if pending:
            try:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass

        loop.close()
        logger.info("Pipeline closed cleanly.")
        sys.exit(0)
if __name__ == "__main__":
    app()