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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.


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

CONF = {}

# Some exceptions our configuration parsing can throw
class MissingRequiredConfigValue(ValueError):
    def __init__(self, what):
        self.what = what
        super(MissingRequiredConfigValue, self).__init__()
    def __str__(self):
        s = 'A value for "%s" must be set in the "broker"' % self.what
        s = s + ' section of your config file'
        return s

def get_configuration(ui):
    # List of config variables required to be set
    required = [
        'BROKER_HOST',
        'BROKER_USER',
        'BROKER_PASS',

        'MSG_EXCHANGE',

        # Full path to where the repositories are stored
        'BASE_PATH',
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
        'LABEL_PUSH':       'push',    # push, landing, check-in...
        'LABEL_PUSHES':     'pushes',  # plural of the above
        'LABEL_PUSHED':     'pushed',  # past tense of the above
        'LABEL_CHANGESET':  'commit',  # commit, changeset, revision, change...
        'LABEL_CHANGESETS': 'commits', # plural of the above

        # bugs, defects, issues...
        'LABEL_BUGS': 'bugs',

        # Git uses master, hg uses default
        # Allow setting either way in the message routing key
        'LABEL_DEFAULT_BRANCH': 'default',

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

    # Make sure required variables are set
    for req in required:
        if not ui.config('broker', req):
            raise MissingRequiredConfigValue(req)

    # Pull our final configuration together...
    # Set the defaults first.
    # We don't need to transform these as they are set in
    # python above so they should be using the correct types
    for defaultvar in defaults:
        CONF[defaultvar] = defaults[defaultvar]

    # Set what the user has put in the config file, transforming if we need to
    for setvar, value in ui.configitems('broker'):
        if setvar in var_types['boolean']:
            value = ui.configbool('broker', setvar)
        elif setvar in var_types['comma']:
            # Specified a comma delimited value
            value = ui.configlist('broker', setvar)
        elif setvar in var_types['regex']:
            # Specified a regex string, precompile
            raw = ui.config('broker', setvar)
            value = re.compile(raw, re.I)
        else:
            value = ui.config('broker', setvar)

        # Set the config variable
        CONF[setvar] = value

# ------------------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------------------

def send_push_message(connection, data):
    # Construct the routing key from the info in the push
    routing_key = "%s%s.%s%s" % (CONF['MSG_ROUTING_PREFIX'],
                                 CONF['LABEL_PUSH'], 
                                 data['repository'].replace('.', '[:dot:]').replace('/','.'),
                                 CONF['MSG_ROUTING_POSTFIX'])
    routing_key = re.sub(r'\.+', '.', routing_key)
    _send_message(connection, routing_key, data, CONF['MSG_USE_ENVELOPE'])

def send_changeset_message(connection, data):
    if not data['branch'] or data['branch'] == 'default':
        branch = CONF['LABEL_DEFAULT_BRANCH']
    else:
        branch = data['branch']
    # Construct the routing key from the info in the changeset
    routing_key = "%s%s.%s.%s%s" % (CONF['MSG_ROUTING_PREFIX'],
                                    CONF['LABEL_CHANGESET'],
                                    data['repository'].replace('.', '[:dot:]').replace('/','.'),
                                    branch,
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
    m = CONF['REGEX_NAME'].match(os.environ['USER'])
    if m:
        if m.group(1):
            data['who']['name'] = m.group(1).strip()
        if m.group(2):
            data['who']['email'] = m.group(1).strip()

    # Get the time the push happened (now!) in the proper format
    data[CONF['LABEL_PUSHED'] + '_at'] = mercurial.util.datestr(None, CONF['DATE_FORMAT'])

    # The repository is a full path. Cut off to the containing directory
    # if we were given one
    (full_path, xxx)  = os.path.split(repo.path)
    data['repository'] = full_path.replace(CONF['BASE_PATH'], '')
    data['repository'] = re.sub(r'^/', r'', data['repository'])

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
        changesetdata['author']    = {'raw': ctx.user()}
        changesetdata['timestamp'] = ctx.date()
        changesetdata['message']   = ctx.description()
        changesetdata['branch']    = ctx.branch()
        changesetdata['tags']      = ctx.tags()

        # Add in some blank fields that may be filled later
        changesetdata['automated']        = False
        changesetdata['approvers']        = []
        changesetdata['reviewers']        = []
        changesetdata['superreviewers']   = []
        changesetdata['uireviewers']      = []
        changesetdata[CONF['LABEL_BUGS']] = set()
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
        m = CONF['REGEX_NAME'].match(changesetdata['author']['raw'])
        if m:
            if m.group(1):
                changesetdata['author']['name'] = m.group(1).strip()
            if m.group(2):
                changesetdata['author']['email'] = m.group(2).strip()

        # Get all the mentioned bug numbers
        matches = CONF['REGEX_BUG'].findall(changesetdata['message'])
        if matches:
            for bugnum in matches:
                changesetdata[CONF['LABEL_BUGS']].add(bugnum)

        # Some encodings (like JSON) can't handle encoding sets
        changesetdata[CONF['LABEL_BUGS']] = list(changesetdata[CONF['LABEL_BUGS']])

        # Find all the "[type]=[people]" flags 
        # (except for b/bug=#####, as that is found above)
        # TODO: This is ugly and should be more robust
        # TODO: These should really be in plugins and people should
        # be able to write their own custom ones
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
            # See bug 660137
            try:
                newfilectx = ctx.filectx(path)
            except:
                newfilectx = None
            try:
                oldfilectx = repo.changectx(node).filectx(path)
            except:
                oldfilectx = None

            if not newfilectx and not oldfilectx:
                # Removed file
                changesetdata['files']['removed'].append(path)
            elif not oldfilectx or not newfilectx.filerev():
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

    # Read the config
    get_configuration(ui)

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
        print "    Sending the %s message..." % CONF['LABEL_PUSH']
        send_push_message(connection, pushdata)

        # Also send messages for each changeset as consumers may be
        # interested in those as well
        print "    Sending individual %s messages..." % CONF['LABEL_CHANGESET']
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
    print "All messages sent to %s successfully." % CONF['BROKER_HOST']
    return 0;
