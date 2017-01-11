# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------#
#  Copyright © 2015-2016 VMware, Inc. All Rights Reserved.                    #
#                                                                             #
#  Licensed under the BSD 2-Clause License (the “License”); you may not use   #
#  this file except in compliance with the License.                           #
#                                                                             #
#  The BSD 2-Clause License                                                   #
#                                                                             #
#  Redistribution and use in source and binary forms, with or without         #
#  modification, are permitted provided that the following conditions are met:#
#                                                                             #
#  - Redistributions of source code must retain the above copyright notice,   #
#      this list of conditions and the following disclaimer.                  #
#                                                                             #
#  - Redistributions in binary form must reproduce the above copyright        #
#      notice, this list of conditions and the following disclaimer in the    #
#      documentation and/or other materials provided with the distribution.   #
#                                                                             #
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"#
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  #
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE #
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE  #
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR        #
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF       #
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS   #
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN    #
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)    #
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF     #
#  THE POSSIBILITY OF SUCH DAMAGE.                                            #
# ----------------------------------------------------------------------------#
import os
import sys
import json
import inspect
import stat
import fcntl
import thread
import logging
import time
from Queue import Queue
from threading import Thread

from liota.disc_listeners.discovery_listener import DiscoveryListener
from liota.device_comms.mqtt_device_comms import MqttDeviceComms
from liota.lib.identity.edge_system_identity import Identity
from liota.lib.identity.tls_conf import TLSConf
from liota.lib.transports.mqtt import QoSDetails

log = logging.getLogger(__name__)

class MqttListener(DiscoveryListener):
    """
    MqttListener does inter-process communication (IPC), and
    subscribe on topics to receive messages sent by devices.
    """
    def proc_dev_msg(self, payload):
        log.debug("mqtt_sub: proc_dev_msg")
        self.discovery.device_msg_process(payload)

    def callback_msg_proc(self, client, userdata, message):
        log.debug("MQTT received msg: {0}".format(message.payload))
        if message.payload != '':
            try:
                payload = json.loads(message.payload)
            except ValueError, err:
                # json can't be parsed
                log.error('Value: {0}, Error:{1}'.format(message.payload, str(err)))
                return
            Thread(target=self.proc_dev_msg, name="MqttMsgProc_Thread", args=(payload,)).start()

    def __init__(self, mqtt_cfg, name=None, discovery=None):
        super(MqttListener, self).__init__(name=name)
        ip_port_topic = mqtt_cfg[0]
        str_list = ip_port_topic.split(':')
        if str_list[0] == "" or str_list[0] == None:
            log.debug("No ip is specified!")
            self.ip = "127.0.0.1"
        else:
            self.ip = str(str_list[0])
        if str_list[1] == "" or str_list[1] == None:
            log.debug("No port is specified!")
        self.port = int(str_list[1])
        if str_list[2] == "" or str_list[2] == None:
            log.debug("No topic is specified!")
        else:
            self.topic = str(str_list[2])
        log.debug("MqttListener is initialized")
        print "MqttListener is initialized"
        self.discovery = discovery

        cfg_sets = mqtt_cfg[1]
        self.cfg_sets = cfg_sets
        self.flag_alive = True
        self.start()

    def run(self):
        if self.flag_alive:
            # To connect with a TLS enabled MQTT broker
            self.edge_system_identity = Identity(self.cfg_sets[0][1], self.cfg_sets[1][1],
                self.cfg_sets[2][1], self.cfg_sets[3][1], self.cfg_sets[4][1])
            # Encapsulate TLS parameters
            #self.tls_conf = TLSConf(self.cfg_sets[5][1], self.cfg_sets[6][1], self.cfg_sets[7][1])
            self.tls_conf = None
            # Encapsulate QoS related parameters
            #self.qos_details = QoSDetails(self.cfg_sets[8][1], self.cfg_sets[9][1], self.cfg_sets[10][1])
            self.qos_details = None
            # Create MQTT connection object with required params
            self.mqtt_conn = MqttDeviceComms(self.edge_system_identity, self.tls_conf,
                self.qos_details, self.ip, self.port, int(self.cfg_sets[11][1]), False)
            # Add callback methods
            self.mqtt_conn.mqtt_client.client.message_callback_add(self.topic,
                self.callback_msg_proc)
            # Subscribe to channel with preferred QoS level 0, 1 or 2
            # Add network loop method loop_start() to remain on the network in order to receive data
            self.mqtt_conn.subscribe(self.topic, 2)
            log.debug("MqttListener is running")
            print "MqttListener is running"
            self.mqtt_conn.mqtt_client.client.loop_forever()
        else:
            log.info("Thread exits: %s" % str(self.name))
            #self.mqtt_conn.mqtt_client.client.loop_stop()
            self.mqtt_conn.mqtt_client.client.disconnect()

    def clean_up(self):
        self.flag_alive = False
        self.mqtt_conn.mqtt_client.client.disconnect()