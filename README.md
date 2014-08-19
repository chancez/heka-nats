Heka-Nats
=========

This is a publish/subscribe input as well as a publish output, for [Heka](http://hekad.readthedocs.org/).

To use it, you need to first add it to build heka with it. You can do this by
adding the following lines to `cmake/plugin_loader.cmake`:

````
git_clone(https://github.com/apcera/nats 1929bb9e89d4956fa98ca93ee8f15a7852532e64)
add_external_plugin(git https://github.com/ecnahc515/heka-nats master)
````

Refer to Heka's offical [Building External Plugins]
(http://hekad.readthedocs.org/en/latest/installing.html#build-include-externals)
 docs for more details.




Configuration
=============

NatsInput
---------

Connects to a remote natsd server and subscribes to a particular subject for
input.

Config:

* url (string): A nats connection string (ex: nats://localhost:4222).
* servers (list of strings, optional): A list of server addresses for the
client to connect to.
* no_randomize (boolean): Whether or not to randomly select the server it
connects to from the server list. Defaults to false.
* subject (string): The subject to subscribe to. This subject is the input to
the plugin.
* reconnect (boolean, optional): Reconnect after being disconnected. Defaults to
false.
* max_reconnects (integer, optional): The maximum number of attempts the input
will try before no longer attempting to connect.
* reconnect_wait (integer, optional): The time in seconds to wait between
reconnect attempts.
* timeout (integer, optional): The time in milliseconds to wait before timing out.
* decoder (string): The decoder name to transform a raw message body into a
structured hekad message.

NatsOuput
---------

Connects to a remote natsd server and publishes output to a specific nats
subject.

Config:

* url (string): A nats connection string (ex: nats://localhost:4222).
* servers (list of strings, optional): A list of server addresses for the
client to connect to.
* no_randomize (boolean): Whether or not to randomly select the server it
connects to from the server list. Defaults to false.
* subject (string): The subject to publish messages as.
* reconnect (boolean, optional): Reconnect after being disconnected. Defaults to
false.
* max_reconnects (integer, optional): The maximum number of attempts the input
will try before no longer attempting to connect.
* reconnect_wait (integer, optional): The time in seconds to wait between
reconnect attempts.
* timeout (integer, optional): The time in milliseconds to wait before timing out.
