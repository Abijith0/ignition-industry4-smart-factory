# Industry 4.0 Smart Factory (Ignition + MQTT + Edge AI)

## Overview

This project demonstrates a basic Industry 4.0 workflow using:

- Ignition SCADA for simulation and visualization  
- MQTT (Mosquitto) for data transfer  
- Python for anomaly detection  
- MySQL for storing production data  

A simulated machine in Ignition publishes data over MQTT.  
A Python script subscribes to this data, detects anomalies, and publishes alerts.

---

## Architecture

```mermaid
graph LR
Machine --> Ignition
Ignition --> MQTT
MQTT --> Mosquitto
Mosquitto --> Python
Python --> MQTT
