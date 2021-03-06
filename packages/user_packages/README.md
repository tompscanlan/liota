# Liota User Packages

Basic user packages which we believe should exist on the edge system to publish the basic health stats to Pulse IoT Control Center

* general_edge_system.py

The sample package contains specifications of GeneralEdgeSystem, replace the "EdgeSystemName" with the logic to auto-generate the unique name everytime this package
is loaded based on the user flow. Unique identifier can be from the system(MAC address) or devices to generate the unique name.

* iotcc_mqtt.py

This is a sample user package which creates a IoTControlCenter DCC object and registers edge system on
IoTCC over MQTT Protocol to acquire "registered edge system", i.e. iotcc_edge_system. This package has dependency on credentials package
which is pre-loaded during the installation in order to provide the required credentials and configuration parameters in the liota package manager registry.
System Properties which are pre-loaded in the registry during installation are required to be set for the registered edge system before the object is stored in the package manager registry,
all the devices will internally inherit the the system properties from the registered edge system.
The properties for the edge system can be set as 'key:value' pair, you can also set the location by passing the
'latitude:value' and 'longitude:value' as properties in the user package.

* iotcc_mqtt_edge_system_stats.py

This is a sample user package to publish the basic edge system stats which we believe are required to
monitor the health status of the edge system from Pulse IoT Control Center.
