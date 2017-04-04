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

from liota.core.package_manager import LiotaPackage

dependencies = ["edge_systems/dell5k/edge_system"]


class PackageClass(LiotaPackage):
    """
    This is a sample package which creates a IoTControlCenter DCC object and registers edge system on
    IoTCC over MQTT Protocol to acquire "registered edge system", i.e. iotcc_edge_system.
    """

    def run(self, registry):
        import copy
        from liota.lib.utilities.identity import Identity
        from liota.dccs.iotcc import IotControlCenter
        from liota.dcc_comms.mqtt_dcc_comms import MqttDccComms
        from liota.dccs.dcc import RegistrationFailure

        # Get values from configuration file
        config_path = registry.get("package_conf")
        config = {}
        execfile(config_path + '/sampleProp.conf', config)

        # Acquire resources from registry
        # Creating a copy of edge_system object to keep original object "clean"
        edge_system = copy.copy(registry.get("edge_system"))

        #  Encapsulates Identity
        identity = Identity(root_ca_cert=None, username=config['broker_username'], password=config['broker_password'],
                            cert_file=None, key_file=None)

        # Initialize DCC object with MQTT transport
        self.iotcc = IotControlCenter(config['broker_username'], config['broker_password'],
                                      MqttDccComms(edge_system_name=edge_system.name,
                                                   url=config['BrokerIP'], port=config['BrokerPort'], identity=identity,
                                                   enable_authentication=True,
                                                   clean_session=True))

        try:
            # Register edge system (gateway)
            iotcc_edge_system = self.iotcc.register(edge_system)
            """
            Use iotcc & iotcc_edge_system as common identifiers
            in the registry to easily refer the objects in other packages
            """
            registry.register("iotcc_mqtt", self.iotcc)
            registry.register("iotcc_edge_system_mqtt", iotcc_edge_system)
        except RegistrationFailure:
            print "EdgeSystem registration to IOTCC failed"
        self.iotcc.set_properties(iotcc_edge_system, config['SystemPropList'])

    def clean_up(self):
        self.iotcc.comms.client.disconnect()
