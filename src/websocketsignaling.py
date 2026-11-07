from typing import Union, Optional
import aiohttp, asyncio

from aiortc import (
    RTCConfiguration,
    RTCIceCandidate,
    RTCIceServer,
    RTCSessionDescription,
)
from aiortc.contrib.signaling import _SignalingObject, BYE
from aiortc.sdp import candidate_from_sdp, candidate_to_sdp

from pprint import pprint
import json
import re

SignalingObject = _SignalingObject | RTCConfiguration

from pyalarmdotcomajax import AlarmBridge
from pyalarmdotcomajax.exceptions import (
    AuthenticationFailed,
    MustConfigureMfa,
    NotAuthorized,
    OtpRequired,
    UnexpectedResponse,
)

# Setup logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocketsignaling")



class WebsocketSignaling:
    def __init__(self, webSock: aiohttp.ClientWebSocketResponse):
        self._websocket: aiohttp.ClientWebSocketResponse = webSock
        self.myId = ""
        self.remoteId = ""

    async def connect(username: str,
                      password: str,
                      cookie: str,
                      ) -> AlarmBridge:

        #####################
        # INITIALIZE BRIDGE #
        #####################

        bridge = AlarmBridge()

        bridge.auth_controller.set_credentials(username, password, cookie)

        # Typer calls bridge.close() when context is closed.
        #ctx.call_on_close(partial(asyncio.get_event_loop().run_until_complete, bridge.close()))

        #
        # LOG IN
        #

        try:
            # Initialize the connector
            await bridge.initialize()

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

    async def close(self):
        if self._websocket is not None and self._websocket.closed is False:
            await self.send(None)
            await self._websocket.close()

    async def receive(self):
        try:
            # Use receive() instead of receive_str() to catch all frame types
            msg = await self._websocket.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.data
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                # Gracefully handle the server closing the connection (type 8)
                # You can return None or raise a custom connection closed exception
                return BYE

            elif msg.type == aiohttp.WSMsgType.ERROR:
                # Handle transport-level errors
                print(msg)
                return None
        except asyncio.IncompleteReadError:
            return
        logger.debug("---------")
        logger.debug("Received:")
        logger.debug(data)
        if data.startswith("SESSION_STARTED"):
            data = data.replace("SESSION_STARTED ", "", 1)
        message = json.loads(data)
        logger.debug(message)
        ret = self.object_from_string(data, self)
        # if ret == None:
        #     logger.info("remote host says good bye!")

        return ret

    async def send(self, descr):
        logger.debug("---------")
        logger.debug("Sending:")
        data = self.object_to_string(descr, self)
        logger.debug(data)
        await self._websocket.send_str(data + '\n')

    async def sendCommand(self, prefix, parent, body):
        logger.debug("---------")
        logger.debug("Alt Sending:")
        if parent is not None and parent != "":
            message = {parent : body}
        else:
            message = body
        message["to"] = self.remoteId
        message["from"] = self.myId
        data = json.dumps(message)
        if prefix is not None and prefix != "":
            data = prefix + " " + data
        logger.debug(data)
        await self._websocket.send_str(data + '\n')

    def object_from_string(self, message_str: str, signaler: "WebsocketSignaling") -> SignalingObject:
        message = json.loads(message_str)
        if "type" in message:
            logger.info("message type: " + message["type"])
        if "from" in message:
            signaler.remoteId = message["from"]
        elif "publisher_peer_id" in message:
            signaler.remoteId = message["publisher_peer_id"]
        if "to" in message:
            signaler.myId = message["to"]

        if "iceServers" in message:
            config = RTCConfiguration()
            config.iceServers = []
            for ice in message["iceServers"]:
                iceServer = RTCIceServer(ice["urls"])
                if "username" in ice:
                    iceServer.username = ice["username"]
                if "credential" in ice:
                    iceServer.credential = ice["credential"]
                config.iceServers.append(iceServer)
            return config
        elif "sdp" in message and message["sdp"]["type"] in ["answer", "offer"]:
            sdp = RTCSessionDescription(**(message["sdp"]))
            #This is a hack.
            sdp.sdp = re.sub("profile-level-id=.{6}", "profile-level-id=42e01f", sdp.sdp)
            return sdp
        elif "ice" in message:
            rawCandidate = message["ice"]["candidate"]
            if ":" in rawCandidate:
                candidate = candidate_from_sdp(rawCandidate.split(":", 1)[1])
            else:
                return None
            if "sdpMid" in message["ice"]:
                candidate.sdpMid = message["ice"]["sdpMid"]
            if "sdpMLineIndex" in message["ice"]:
                candidate.sdpMLineIndex = message["ice"]["sdpMLineIndex"]
            #candidate.usernameFragment = message["usernameFragment"]
            return candidate
        elif "type" in message and message["type"] == "bye":
            return BYE
        elif "error" in message:
            print("Error key:", message_str)
            return message
        else:
            print("Others:", message_str)
            return message
            # assert message["type"] == "bye"
            # return BYE


    def object_to_string(self, obj: SignalingObject, signaler: "WebsocketSignaling") -> str:
        message: dict[str, Union[int, str]]
        if isinstance(obj, RTCSessionDescription):
            message = {"sdp": {"sdp": obj.sdp, "type": obj.type}}
        elif isinstance(obj, RTCIceCandidate):
            message = {"ice": {
                "candidate": "candidate:" + candidate_to_sdp(obj),
                "id": obj.sdpMid,
                "label": obj.sdpMLineIndex,
                "type": "candidate",
            }}
        else:
            assert obj is BYE
            message = {"type": "bye"}
        message["to"] = signaler.remoteId
        message["from"] = signaler.myId
        #logger.debug(message)
        return json.dumps(message, sort_keys=True)