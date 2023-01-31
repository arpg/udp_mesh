# UDP Mesh

UDP Mesh is a ROS transport layer that implements reliable UDP transport using only unicast and broadcast UDP datagrams. The package is specifically designed to be used as an application layer on top of a mesh layer.

UDP mesh encapsulates messages from existing ROS nodes without performing serialization and deserialization for improved performance.

UDP mesh performs discovery using a periodic heartbeat which advertises the availability of a given node on the mesh. The heartbeat interval is controlled by the `mesh_heartbeat_interval` parameter (in seconds).

To get started modify the `rudp_mesh_node.launch` file to match your system:

```
mesh_data_port: UDP port to use (default: 8700)
mesh_interface: Physical interface connected to the mesh network (e.g wlan0, eth0, or similar)
topic_list: List of topics and priorities for udp mesh to transmit:

Example Value:
"[1|std_msgs/String|high_pri_topic,
	4|std_msgs/Bool|low_pri_topic,
	8|sensor_msgs/CameraInfo|cam_info_topic,
	8|sensor_msgs/Image|image_raw]"

A lower number represents a higher priority.
```

UDP Mesh was originally designed to run on ROS Melodic. The `python3` branch should work on Ros Noetic.

## Papers
The following papers provide more details on UDP Mesh. If you find this work helpful or use any of this code, please cite one or more of the following works:

**Heterogeneous Ground-Air Autonomous Vehicle Networking in Austere Environments: Practical Implementation of a Mesh Network in the DARPA Subterranean Challenge**

```
@inproceedings{biggie2022heterogeneous,
  title={Heterogeneous Ground-Air Autonomous Vehicle Networking in Austere Environments: Practical Implementation of a Mesh Network in the DARPA Subterranean Challenge},
  author={Biggie, Harel and McGuire, Steve},
  booktitle={2022 18th International Conference on Distributed Computing in Sensor Systems (DCOSS)},
  pages={261--268},
  year={2022},
  organization={IEEE}
}
```

**Flexible Supervised Autonomy for Exploration in
Subterranean Environments**

```
@article{biggie2023flexible,
  title={Flexible supervised autonomy for exploration in subterranean environments},
  author={Biggie, Harel and Rush, Eugene R and Riley, Danny G and Ahmad, Shakeeb and Ohradzansky, Michael T and Harlow, Kyle and Miles, Michael J and Torres, Daniel and McGuire, Steve and Frew, Eric W and others},
  journal={arXiv preprint arXiv:2301.00771},
  year={2023}
}
```

