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

import json
import logging
import time
import threading
import ConfigParser
import os
from time import gmtime, strftime

from liota.dccs.dcc import DataCenterComponent, RegistrationFailure
from liota.lib.protocols.helix_protocol import HelixProtocol
from liota.entities.metrics.metric import Metric
from liota.lib.utilities.utility import LiotaConfigPath, getUTCmillis, mkdir_log
from liota.lib.utilities.si_unit import parse_unit
from liota.entities.metrics.registered_metric import RegisteredMetric
from liota.entities.registered_entity import RegisteredEntity

log = logging.getLogger(__name__)


class IotControlCenter(DataCenterComponent):
    """ The implementation of IoTCC cloud provider solution

    """

    def __init__(self, username, password, con):
        log.info("Logging into DCC")
        self.comms = con
        self.con = con.wss
        self.username = username
        self.password = password
        self.proto = HelixProtocol(self.con, username, password)

        self.dev_file_path = self._get_dev_file_path()

        def on_receive_safe(msg):
            try:
                log.debug("Received msg: {0}".format(msg))
                json_msg = json.loads(msg)
                self.proto.on_receive(json.loads(msg))
                log.debug("Processed msg: {0}".format(json_msg["type"]))
                if json_msg["type"] == "connection_verified":
                    log.info("Connection verified")
                    exit()
            except Exception:
                raise
                log.exception(
                    "Error received on connecting to DCC instance. Please verify the credentials and try again.")

        thread = threading.Thread(target=self.con.run)
        self.con.on_receive = on_receive_safe
        thread.daemon = True
        thread.start()
        thread.join()
        log.info("Logged in to DCC successfully")

    def register(self, entity_obj):
        """ Register the objects

        """
        if isinstance(entity_obj, Metric):
            # reg_entity_id should be parent's one: not known here yet
            # will add in creat_relationship(); publish_unit should be done inside
            return RegisteredMetric(entity_obj, self, None)
        else:
            # finally will create a RegisteredEntity
            log.info("Registering resource with IoTCC {0}".format(entity_obj.name))

            def on_receive_safe(msg):
                try:
                    log.debug("Received msg: {0}".format(msg))
                    if msg != "":
                        json_msg = json.loads(msg)
                        self.proto.on_receive(json.loads(msg))
                        log.debug("Processed msg: {0}".format(json_msg["type"]))
                        if json_msg["type"] == "create_or_find_resource_response":
                            if json_msg["body"]["uuid"] != "null":
                                log.info("FOUND RESOURCE: {0}".format(json_msg["body"]["uuid"]))
                                self.reg_entity_id = json_msg["body"]["uuid"]
                                exit()
                            else:
                                log.info("Waiting for resource creation")
                                time.sleep(5)
                                self.con.send(
                                    self._registration(self.con.next_id(), entity_obj.entity_id, entity_obj.name,
                                                       entity_obj.entity_type))
                except:
                    raise

            thread = threading.Thread(target=self.con.run)
            self.con.on_receive = on_receive_safe
            thread.daemon = True
            thread.start()
            if entity_obj.entity_type == "EdgeSystem":
                entity_obj.entity_type = "HelixGateway"
            self.con.send(
                self._registration(self.con.next_id(), entity_obj.entity_id, entity_obj.name, entity_obj.entity_type))
            thread.join()
            if not hasattr(self, 'reg_entity_id'):
                raise RegistrationFailure()
            log.info("Resource Registered {0}".format(entity_obj.name))
            if entity_obj.entity_type == "HelixGateway":
                self.store_reg_entity_attributes("EdgeSystem", entity_obj.name,
                    self.reg_entity_id, None, None)
            else:
                # get dev_type, and prop_dict if possible
                self.store_reg_entity_attributes("Devices", entity_obj.name, self.reg_entity_id,
                    entity_obj.entity_type, None)

            return RegisteredEntity(entity_obj, self, self.reg_entity_id)

    def create_relationship(self, reg_entity_parent, reg_entity_child):
        """ This function initializes all relations between Registered Entities.

            Parameters:
            - obj: The object that has just obtained an UUID
        """
        # sanity check: must be RegisteredEntity or RegisteredMetricRegisteredMetric
        if (not isinstance(reg_entity_parent, RegisteredEntity) \
            and not isinstance(reg_entity_parent, RegisteredMetric)) \
            or (not isinstance(reg_entity_child, RegisteredEntity) \
                and not isinstance(reg_entity_child, RegisteredMetric)):
            raise TypeError()

        reg_entity_child.parent = reg_entity_parent
        if isinstance(reg_entity_child, RegisteredMetric):
            # should save parent's reg_entity_id
            reg_entity_child.reg_entity_id = reg_entity_parent.reg_entity_id
            entity_obj = reg_entity_child.ref_entity
            self.publish_unit(reg_entity_child, entity_obj.name, entity_obj.unit)
        else:
            self.con.send(self._relationship(self.con.next_id(),
                reg_entity_parent.reg_entity_id, reg_entity_child.reg_entity_id))

    def _registration(self, msg_id, res_id, res_name, res_kind):
        return {
            "transactionID": msg_id,
            "type": "create_or_find_resource_request",
            "body": {
                "kind": res_kind,
                "id": res_id,
                "name": res_name
            }
        }

    def _relationship(self, msg_id, parent_res_uuid, child_res_uuid):
        return {
            "transactionID": msg_id,
            "type": "create_relationship_request",
            "body": {
                "parent": parent_res_uuid,
                "child": child_res_uuid
            }
        }

    def _properties(self, msg_id, res_uuid, res_kind, timestamp, properties):
        msg = {
            "transationID": msg_id,
            "type": "add_properties",
            "uuid": res_uuid,
            "body": {
                "kind": res_kind,
                "timestamp": timestamp,
                "property_data": []
            }
        }
        for key, value in properties.items():
            msg["body"]["property_data"].append({"propertyKey": key, "propertyValue": value})
        return msg

    def _get_properties(self, msg_id, res_uuid):
        return {
            "transactionID": msg_id,
            "type": "get_properties",
            "uuid": res_uuid
        }

    def _format_data(self, reg_metric):
        met_cnt = reg_metric.values.qsize()
        if met_cnt == 0:
            return
        _timestamps = []
        _values = []
        for _ in range(met_cnt):
            m = reg_metric.values.get(block=True)
            if m is not None:
                _timestamps.append(m[0])
                _values.append(m[1])
        if _timestamps == []:
            return
        return {
            "type": "add_stats",
            "uuid": reg_metric.reg_entity_id,
            "metric_data": [{
                "statKey": reg_metric.ref_entity.name,
                "timestamps": _timestamps,
                "data": _values
            }],
        }

    def set_properties(self, reg_entity_obj, properties):
        # RegisteredMetric get parent's resid; RegisteredEntity gets own resid
        reg_entity_id = reg_entity_obj.reg_entity_id

        if isinstance(reg_entity_obj, RegisteredMetric):
            entity = reg_entity_obj.parent.ref_entity
        else:
            entity = reg_entity_obj.ref_entity

        log.info("Properties defined for resource {0}".format(entity.name))
        self.con.send(
            self._properties(self.con.next_id(), reg_entity_id, entity.entity_type,
                             getUTCmillis(), properties))
        if entity.entity_type == "HelixGateway":
            self.store_reg_entity_attributes("EdgeSystem", entity.name,
                reg_entity_obj.reg_entity_id, None, properties)
        else:
            # get dev_type, and prop_dict if possible
            self.store_reg_entity_attributes("Devices", entity.name, reg_entity_obj.reg_entity_id,
                entity.entity_type, properties)

    def publish_unit(self, reg_entity_obj, metric_name, unit):
        str_prefix, str_unit_name = parse_unit(unit)
        if not isinstance(str_prefix, basestring):
            str_prefix = ""
        if not isinstance(str_unit_name, basestring):
            str_unit_name = ""
        properties_added = {
            metric_name + "_unit": str_unit_name,
            metric_name + "_prefix": str_prefix
        }
        self.set_properties(reg_entity_obj, properties_added)
        log.info("Published metric unit with prefix to IoTCC")


    def store_edge_system_info(self, uuid, name, prop_list):
        import lxml.etree as etree

        """
        create (can overwrite) edge system info file of UUID.xml, with format of
        <attributes>
        <attribute name=attribute name value=attribute value/>
        …
        </attributes>
        except the first attribute is edge system name, all other attributes may vary
        """
        log.debug("store_edge_system_info")
        log.debug('{0}:{1}, prop_list: {2}'.format(uuid, name, prop_list))
        root = etree.Element("attributes")
        # add edge system name as an attribute
        child = etree.SubElement(root, "attribute")
        attributes = child.attrib
        attributes["name"] = "edge system name"
        attributes["value"] = name
        # add edge system properties as attributes
        if prop_list is not None:
            for i, dict in enumerate(prop_list):
                for key, value in dict.items():
                    child1 = etree.SubElement(root, "attribute")
                    attributes = child1.attrib
                    attributes["name"] = key
                    attributes["value"] = value
        # add time stamp
        child1 = etree.SubElement(root, "attribute")
        attributes = child1.attrib
        attributes["name"] = "LastSeenTimestamp"
        attributes["value"] = strftime("%Y-%m-%dT%H:%M:%S", gmtime())

        et = etree.ElementTree(root)
        log.debug("store_edge_system_info dev_file_path:{0}".format(self.dev_file_path))
        file_path = self.dev_file_path + '/' + uuid + '.xml'
        et.write(file_path, pretty_print=True)

        return

    def store_device_info(self, uuid, name, dev_type, prop_list):
        """
        create (can overwrite) device info file of device_UUID.json, with format of
        {
            "discovery":  {
                "remove" : false,
                "attributes": [
                    {"IoTDeviceType" : "LM35"},
                    {"IoTDeviceName" : "LM35-12345"},
                    {"model": "LM35-A2"},
                    {"function": "thermistor"},
                    {"port" : "GPIO-3"},
                    {"manufacturer" : "Texas Instrument"},
                    {"LastSeenTimestamp" : "04 NOV 2016"}
                ]
            }
        }
        except IoTDeviceType and IoTDeviceName, all other attributes may vary
        """
        log.debug("store_device_info")
        log.debug('prop_dict: {0}'.format(prop_list))
        attribute_list = [{"IoTDeviceType": dev_type},
                    {"IoTDeviceName": name}]
        # attribute_list.append(prop_dict)
        if prop_list is not None:
            for i, dict in enumerate(prop_list):
                for key, value in dict.items():
                    log.debug("prop_list:(%s : %s)\n" % (key, value))
                    attribute_list.append({key: value})
        attribute_list.append({"LastSeenTimestamp": strftime("%Y-%m-%dT%H:%M:%S", gmtime())})
        log.debug('attribute_list: {0}'.format(attribute_list))
        msg = {
            "discovery":  {
                "remove" : False,
                "attributes": attribute_list
            }
        }
        log.debug('msg: {0}'.format(msg))
        log.debug("store_device_info dev_file_path:{0}".format(self.dev_file_path))
        file_path = self.dev_file_path + '/' + uuid + '.json'
        try:
            with open(file_path, 'w') as f:
                json.dump(msg, f, sort_keys = True, indent = 4, ensure_ascii=False)
                log.debug('Initialized ' + file_path)
            f.close()
        except IOError, err:
            log.error('Could not open {0} file '.format(file_path) + err)

    def store_reg_entity_attributes(self, entity_type, entity_name, reg_entity_id,
        dev_type, prop_dict):

        log.debug('store_reg_entity_attributes\n {0}:{1}:{2}:{3}'.format(entity_type,
             entity_name, reg_entity_id, prop_dict))

        # need to get a complete property list (B) first from iotcc
        # if prop_dict (A) is not None, i.e., it is called from set_properties,
        # need to get properties multiple times, until all (k,v) in A are also in B,
        #  that is, set_properties has been really completed.
        # Then, write B to file
        get_prop_cnt = 0
        if prop_dict is None: # means get_properties is called after registration
            get_prop_cnt_limit = 1
        else:
            get_prop_cnt_limit = 5
        self.prop_list = None
        while get_prop_cnt < get_prop_cnt_limit:
            log.debug('sleep 1 sec before getting properties')
            time.sleep(1)
            self.get_properties(reg_entity_id)
            prop_list = self.prop_list
            get_prop_cnt += 1
            if prop_list is None:
                log.info("403 prop_list is None:{0}".format((prop_list is None)))
                continue
            if prop_list == "":
                log.info("406 prop_list is None:{0}".format((prop_list == "")))
                continue
            if prop_dict is None:
                log.info("prop_dict is None")
                break
            # check all (k, v) in prop_dict are the same as in prop_list
            complete_list_flag = True
            for k in prop_dict.iterkeys():
                v = prop_dict[k]
                fnd_key = False
                for index, item in enumerate(prop_list):
                    if k in item.keys():
                        fnd_key = True
                        v_in_list = item[k]
                        break
                if fnd_key == False:
                    # new key in prop_dict
                    complete_list_flag = False
                    break # continue while loop to get_properties again
                else:
                    if v_in_list != v:
                        # value is not updated in prop_list
                        complete_list_flag = False
                        break # continue while loop to get_properties again
            if not complete_list_flag:
                log.debug("NOT complete property list")
                time.sleep(1)
                continue # while
            else:
                log.debug("IS complete property list")
                break # found completed

        if (get_prop_cnt >= get_prop_cnt_limit) and (prop_dict is not None):
            # not complete list, need to merge to get new list
            prop_list = self.merge_prop_list(prop_dict, prop_list)
        if entity_type == "EdgeSystem":
            self.store_edge_system_info(reg_entity_id, entity_name, prop_list)
        elif entity_type == "Devices":
            self.store_device_info(reg_entity_id, entity_name, dev_type, prop_list)
        else:
            return

    def merge_prop_list(self, prop_dict, prop_list):
        # prop_dict: new property dictionary (must not be null)
        # prop_list: list of dictionary items (must not be null)
        if (prop_dict is None):
            return prop_list
        if (prop_list is None):
            return prop_dict
        for k in prop_dict.iterkeys():
            v = prop_dict[k]
            fnd_key = False
            for index, item in enumerate(prop_list):
                if k in item.keys():
                    fnd_key = True
                    if (item[k] != v):
                        item[k] = v
                        break
            if fnd_key == False:
                # new key in prop_dict, add to prop_list
                prop_list.append({k, v})
        # get updated list
        return prop_list

    def _get_dev_file_path(self):

        log.debug("_get_dev_file_path dev_file_path:")
        config = ConfigParser.RawConfigParser()
        fullPath = LiotaConfigPath().get_liota_fullpath()
        if fullPath != '':
            try:
                if config.read(fullPath) != []:
                    try:
                        # retrieve device info file storage directory
                        dev_file_path = config.get('IOTCC_PATH', 'dev_file_path')
                        log.debug("_get_dev_file_path dev_file_path:{0}".format(dev_file_path))
                    except ConfigParser.ParsingError as err:
                        log.error('Could not open config file ' + err)
                        return None
                    if not os.path.exists(dev_file_path):
                        try:
                            os.makedirs(dev_file_path)
                        except OSError as exc:  # Python >2.5
                            if exc.errno == errno.EEXIST and os.path.isdir(dev_file_path):
                                pass
                            else:
                                log.error('Could not create device file storage directory')
                                return None
                    log.debug("_get_dev_file_path dev_file_path:{0}".format(dev_file_path))
                    return dev_file_path
                else:
                    log.error('Could not open config file ' + fullPath)
                    return None
            except IOError, err:
                log.error('Could not open config file')
                return None
        else:
            # missing config file
            log.warn('liota.conf file missing')
            return None

    def get_properties(self, resource_uuid):
        """ get list of properties with resource uuid """

        log.info("Get properties defined with IoTCC for resource {0}".format(resource_uuid))
        self.prop_list = None
        self.get_cnt = 0
        def on_receive_safe(msg):
            try:
                log.debug("Received msg: {0}".format(msg))
                if msg != "":
                    json_msg = json.loads(msg)
                    self.proto.on_receive(json_msg)
                    log.debug("Processed msg: {0}".format(json_msg["type"]))
                    if json_msg["type"] == "get_properties_response":
                        if json_msg["body"]["uuid"] == resource_uuid:
                            log.info("FOUND PROPERTIE LIST: {0}".format(json_msg["body"]["propertyList"]))
                            self.prop_list = json_msg["body"]["propertyList"]
                            log.info("prop_list:{0}".format(self.prop_list))
                            exit()
                        else:
                            self.get_cnt += 1
                            if self.get_cnt > 5:
                                exit()
                            log.info("Waiting for properties")
                            time.sleep(1)
                            self.con.send(self._get_properties(self.con.next_id(), resource_uuid))
            except:
                raise

        thread = threading.Thread(target=self.con.run)
        self.con.on_receive = on_receive_safe
        thread.daemon = True
        thread.start()
        self.con.send(self._get_properties(self.con.next_id(), resource_uuid))
        thread.join()
        return self.prop_list
