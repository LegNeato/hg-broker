#!/usr/bin/env python

# Copyright (C) 2010 Mozilla Foundation
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.


# This script implements a Mercurial hook to send messages to a message broker
# about mercurial pushes and changesets.
# See the README for how to configure
#
# Contributors:
#   Christian Legnitto <clegnitto@mozilla.com>

import os
import re

from mercurial.node import hex
from mercurial import demandimport
import mercurial.util
import mercurial.config

demandimport.disable()
from carrot.connection import BrokerConnection
from carrot.messaging import Publisher
demandimport.enable()


# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

# Some exceptions our configuration parsing can throw
class NoBrokerConfiguration(Exception):
    def __str__(self):
        return 'Please specify configuration values in a "broker" section' + \
               ' of your config file'

class MissingRequiredBrokerConfigurationValue(Exception):
    def __init__(self, what):
        self.what = what
        super(MissingRequiredBrokerConfigurationValue, self).__init__()
    def __str__(self):
        s = 'A value for "%s" must be set in the "broker"' % self.what
        s = s + ' section of your config file'
        return s

# List of config variables required to be set
required = [
    'BROKER_HOST',
    'BROKER_USER',
    'BROKER_PASS',

    'MSG_EXCHANGE',

    # Full path to where the repositories are stored
    'REPO_ROOT',
]

# Optional config variables and their default values
defaults = {
    'BROKER_PORT':     None,
    'BROKER_PROTOCOL': 'AMQP',

    'MSG_VHOST':           '/',
    'MSG_PERSISTENT':      True,
    'MSG_USE_ENVELOPE':    True,
    'MSG_ROUTING_PREFIX':  'hg.',
    'MSG_ROUTING_POSTFIX': '',

    'HG_FAIL_ON_MSG_FAIL': False,

    # Some terminology may be different for people
    # Currently these match the GitHub service hook
    'LABEL_PUSH':       'push',    # push, landing, check-in, etc
    'LABEL_PUSHES':     'pushes',  # plural of the above
    'LABEL_PUSHED':     'pushed',  # past tense of the above
    'LABEL_CHANGESET':  'commit',  # commit, changeset, revision, change, etc
    'LABEL_CHANGESETS': 'commits', # plural of the above

    'DATE_FORMAT': '%Y-%m-%dT%H:%M:%S%z',

    # For extracting data from pushes/changes
    'REGEX_NAME':   re.compile(r'^(.*)\s*<(.*)>\s*$', re.I),
    'REGEX_CLOSED': re.compile(r'closed\s*tree', re.I),
    'REGEX_BUG':    re.compile(r'\d{4,}', re.I),
    'REGEX_FLAGS':  re.compile(r'(approval|review|reviewed|super|ui-review|ui|[ars,/]+|a[0-9\.]+)=(\w+,?/?\w+)', re.I),

    'AUTOMATED_USERS': [],
}

# The ini doesn't have type information so we may need to translate
var_types = {
    'boolean': [
                   'MSG_PERSISTENT',
                   'MSG_USE_ENVELOPE',
                   'HG_FAIL_ON_MSG_FAIL',
               ],
    'comma':   [
                   'AUTOMATED_USERS', 
               ],
    'regex':   [
                   'REGEX_NAME',
                   'REGEX_CLOSED',
                   'REGEX_BUG',
                   'REGEX_FLAGS',
               ],
    # Everything else is considered a string
}

# Load all the config files
config_paths = mercurial.util.rcpath()
hgconf = mercurial.config.config()
for path in config_paths:
    try:
        hgconf.read(path)
    except IOError:
        pass

# Make sure there is a broker section defined somewhere
if 'broker' not in hgconf:
    raise NoBrokerConfiguration

# Make sure required variables are set
for req in required:
    if req not in hgconf['broker']:
        raise MissingRequiredBrokerConfigurationValue(req)


# Pull our final configuration together...
CONF = {}

# Set the defaults first. We don't need to transform these as they are set in
# python above so they should be using the correct types
for defaultvar in defaults:
    CONF[defaultvar] = defaults[defaultvar]

# Set what the user has put in the config file, transforming if we need to
for setvar in hgconf['broker']:
    value = hgconf['broker'][setvar]
    if setvar in var_types['boolean']:
        # Specified a boolean variable
        if isinstance(value, basestring) and \
           value.lower() in ['0','false','no']:
            CONF[setvar] = False
        else:
            CONF[setvar] = bool(value)
    elif setvar in var_types['comma']:
        # Specified a comma delimited value
        CONF[setvar] = re.split(r',\s*', value)
    elif setvar in var_types['regex']:
        # Specified a regex string, precompile
        CONF[setvar] = re.compile(value, re.I)
    else:
        # Default to a string / setting the variable directly
        CONF[setvar] = value


# ------------------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------------------

def send_push_message(connection, data):
    # Construct the routing key from the repo info in the push
    routing_key = "%s%s.%s%s" % (CONF['MSG_ROUTING_PREFIX'],
                                 CONF['LABEL_PUSH'], 
                                 data['repository'].replace(CONF['REPO_ROOT'], '').replace('/','.'),
                                 CONF['MSG_ROUTING_POSTFIX'])
    routing_key = re.sub(r'\.+', '.', routing_key)
    _send_message(connection, routing_key, data, CONF['MSG_USE_ENVELOPE'])

def send_changeset_message(connection, data):
    # Construct the routing key from the repo info in the push
    routing_key = "%s%s.%s%s" % (CONF['MSG_ROUTING_PREFIX'],
                                 CONF['LABEL_CHANGESET'],
                                 data['repository'].replace(CONF['REPO_ROOT'], '').replace('/','.'),
                                 CONF['MSG_ROUTING_POSTFIX'])
    routing_key = re.sub(r'\.+', '.', routing_key)
    _send_message(connection, routing_key, data, CONF['MSG_USE_ENVELOPE'])

def _send_message(connection, routing_key, data, envelope):

    # Make a message envelope if we are told to have one
    if envelope:
        message = {}
        message['payload'] = data
        message['_meta'] = {
            'exchange':    CONF['MSG_EXCHANGE'],
            'routing_key': routing_key,
            'sent': mercurial.util.datestr(None, CONF['DATE_FORMAT']),
            # TODO: Support more than just JSON
            'serializer': 'json'
        }
    else:
        message = data

    # Set up our broker publisher
    publisher = Publisher(connection=connection,
                          exchange=CONF['MSG_EXCHANGE'],
                          exchange_type="topic",
                          routing_key=routing_key)

    # Send the message
    # TODO: Support more than just JSON
    publisher.send(message)

    # Close the publishing connection
    publisher.close()

def get_push_data(ui, repo, node):

    data = {}

    # XXX: Is this the correct way to get the user?
    # Not sure, but it's the way the pushlog hook does it
    data['who']  = {'raw': os.environ['USER']}
    data[CONF['LABEL_PUSHED'] + '_at'] = mercurial.util.datestr(None, CONF['DATE_FORMAT'])

    # TODO: Figure out how to get this info without manually
    # setting the root
    data['repository'] = repo.path.replace(CONF['REPO_ROOT'], '')

    # By default we have no changesets
    data[CONF['LABEL_CHANGESETS']] = []

    # All changesets from node to 'tip' inclusive are part of this push
    rev = repo.changectx(node).rev()
    tip = repo.changectx('tip').rev()
    for i in range(rev, tip+1):
        ctx = repo.changectx(i)

        changesetdata = {}
        changesetdata['revision']  = ctx.rev()
        changesetdata['node']      = hex(ctx.node())
        changesetdata['author']    = ctx.user()
        changesetdata['timestamp'] = ctx.date()
        changesetdata['message']   = ctx.description()
        changesetdata['branch']    = ctx.branch()
        changesetdata['tags']      = ctx.tags()

        # Add in some blank fields that may be filled later
        changesetdata['automated']      = False
        changesetdata['approvers']      = []
        changesetdata['reviewers']      = []
        changesetdata['superreviewers'] = []
        changesetdata['uireviewers']    = []
        changesetdata['bugs']           = set()
        changesetdata['files'] = {
            'added':    [],
            'modified': [],
            'removed':  [],
        }

        # Do some special processing...

        # Mark as automated if the user is an automated user
        if changesetdata['author'] in CONF['AUTOMATED_USERS']:
            changesetdata['automated'] = True

        # Convert dates
        datekeys = ['timestamp']
        for x in datekeys:
            if x in changesetdata:
                changesetdata[x] = mercurial.util.datestr(changesetdata[x],
                                                          CONF['DATE_FORMAT'])

        # Parse out some user info so consumers don't have to
        usertmp = {'raw': changesetdata['author']}
        m = CONF['REGEX_NAME'].match(changesetdata['author'])
        if m:
            if m.group(1):
                usertmp['name'] = m.group(1).strip()
            if m.group(2):
                usertmp['email'] = m.group(2).strip()
        changesetdata['author'] = usertmp

        # Get all the mentioned bug numbers
        matches = CONF['REGEX_BUG'].findall(changesetdata['message'])
        if matches:
            for bugnum in matches:
                changesetdata['bugs'].add(bugnum)

        # Some encodings (like JSON) can't handle encoding sets
        changesetdata['bugs'] = list(changesetdata['bugs'])

        # Find all the "[type]=[people]" flags 
        # (except for b/bug=#####, as that is found above)
        # TODO: This is ugly and should be more robust
        matches = CONF['REGEX_FLAGS'].findall(changesetdata['message'])
        if matches:
            for (what, who) in matches:
                keys = []

                if what.count('a') or what == 'approval':
                    keys.append('approvers')

                if what.count('u') or what == 'ui-review':
                    keys.append('uireviewers')

                if what.count('s/') or what.count('sr') or what == 'super':
                    keys.append('superreviewers')

                if what == 'r' or what == 'review' or \
                   what == 'reviewed' or what.count('/r') or \
                   re.match(r'[^s]*r/', what):
                    keys.append('reviewers')

                people = re.split(r'[,/]+', who)
                for key in keys:
                    for x in people:
                        changesetdata[key].append(x.strip())

        # File processing
        # TODO: Do we want to include additional metadata and/or diffs?
        for path in ctx.files():

            # Get the file contexts
            newfilectx = ctx.filectx(path)
            oldfilectx = repo.changectx(node).filectx(path)

            if not oldfilectx or not newfilectx.filerev():
                # Added file
                changesetdata['files']['added'].append(path)
            elif not newfilectx or \
                (newfilectx.size() == 0 and oldfilectx.size() != 0):
                # Removed file
                changesetdata['files']['removed'].append(path)
            else:
                # Changed file
                changesetdata['files']['modified'].append(path)
                
        data[CONF['LABEL_CHANGESETS']].append(changesetdata)
        
    return data

def send_messages(ui, repo, node, **kwargs):

    # Let the user know what is going on
    print "Sending messages to %s" % CONF['BROKER_HOST']
    print "Please do not interrupt..."

    try:
        # Get the push data
        # TODO: Support pulling from the pushlog db
        pushdata = get_push_data(ui, repo, node);

        # If they don't want AMQP, switch the backend
        if CONF['BROKER_PROTOCOL'] == "STOMP":
            backend = 'carrot.backends.pystomp.Backend'
        else:
            backend = None

        # Connect once for all the messages we will send
        connection = BrokerConnection(hostname=CONF['BROKER_HOST'],
                                      port=CONF['BROKER_PORT'],
                                      userid=CONF['BROKER_USER'],
                                      password=CONF['BROKER_PASS'],
                                      backend_cls=backend)

        # Send the overall push message. Most consumers likely
        # only care about this message
        send_push_message(connection, pushdata)

        # Also send messages for each changeset as consumers may be
        # interested in those as well
        for changedata in pushdata[CONF['LABEL_CHANGESETS']]:
            changedata['repository'] = pushdata['repository']
            send_changeset_message(connection, changedata)

        try:
            connection.close()
        except:
            # We don't care about connection close failures
            pass

    except Exception, e:
        # Something went wrong...
        print "ERROR: Hook returned an exception: %s" % e

        if CONF['HG_FAIL_ON_MSG_FAIL']:
            # Admin wants the hook to fail on the message send failure
            print "Please try pushing again."
            return 1
        else:
            # Admin wants the hook to succeed on the message send failure
            print "Ignoring and continuing the push..."
            return 0

    # Hook succeeded
    print "All messages sent successfully."
    return 0;
