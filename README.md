This is a Mercurial hook to send messages about pushes and commits
to a message broker via AMQP or STOMP.

It is currently only tested with RabbitMQ.


Dependencies
====================
* carrot
* uuid (if using python < 2.6)

Installing
====================

1. Install the dependencies above
2. Copy broker.py somewhere in your PYTHONPATH
3. Edit the "hooks" section of your config file and add:
    pretxnchangegroup.z_broker = python:broker.send_messages
4. Create a "broker" section in your config file
5. The minimum configuration variables that must be set are in
   'example.hrc'

Limitations
====================

1. Assumes the exchange you are publishing to is a topic exchange
2. No SSL support yet
3. The parsing is hardcoded. Eventually I want to have parse plugins

TODO
====================

1. Write some tests
