The docker will install everything you need to have the script work.

However, to setup the connection, you may need a newer pyalarmdotcomajax on your OS to get your MFA cookie from https://github.com/nprzy/pyalarmdotcomajax/tree/fix-mfa-cookie.

After you install pyalarmdotcomajax which has a command line tool:

To setup your account information, you will use the adc command line tool to get a Multi-Factor Authentication cookie.

`adc get -u "[your_username]" -p "[your_password]"

Follow the instructions and the cookie will be given immediately after your OTP entry.


These will provide you with enough information to setup the "alarm_auth:" section of the config file.

The script currently only supports one camera at a time.

The "description" of the camera is the name of the camera that you have given it on the website.
The next two values are likely correct, but can be found in the output of the script if it does not work.

The "service_endpoint" section points the script at your mediamtx instance.
