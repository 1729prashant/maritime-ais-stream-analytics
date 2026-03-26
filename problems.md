# 🚢 AIS Data Engineering – Real-World Problem Statements

Data source: AIS streaming data from AISStream (real-time vessel positions, speed, course, timestamps, vessel metadata)

---

## 1. Maritime Congestion Hotspot Detection

### Problem
Identify regions where vessels are clustering and moving slowly outside ports.

### Data Used
- Latitude / Longitude  
- Speed (SOG)  
- Timestamp  

### Approach
- Filter vessels with low speed (e.g., < 3 knots)
- Apply spatial clustering (e.g., H3 / geohash / DBSCAN)
- Track density changes over time

### Output
- Real-time congestion heatmap  
- Top congestion zones (hourly/daily)

### Business Value
- Early detection of supply chain bottlenecks  
- Route optimization for shipping companies  
- Trade flow forecasting  

---

## 2. Vessel Route Deviation Detection

### Problem
Detect vessels that deviate significantly from typical routes.

### Data Used
- Historical trajectories (lat/lon over time)  
- Current vessel position  

### Approach
- Build baseline routes using historical AIS data  
- Compare live trajectory vs baseline  
- Flag deviations beyond threshold  

### Output
- List of vessels with abnormal routes  
- Deviation score per vessel  

### Business Value
- Identify delays and inefficiencies  
- Detect suspicious or high-risk behavior  
- Improve fleet monitoring  

---

## 3. Transit Time Estimation Between Regions

### Problem
Measure actual travel time between two regions (e.g., shipping corridors).

### Data Used
- Vessel positions  
- Timestamp  

### Approach
- Define geofenced regions  
- Detect entry into region A and region B  
- Compute travel duration  

### Output
- Average transit time  
- Distribution of transit durations  

### Business Value
- Supply chain planning  
- SLA validation  
- Trade route benchmarking  

---

## 4. Vessel Activity Classification (Idle vs Active)

### Problem
Determine how many vessels are active vs idle at any given time.

### Data Used
- Speed (SOG)  
- Position  

### Approach
- Classify vessels:
  - Active → speed > threshold  
  - Idle → speed ≈ 0  
- Aggregate counts over time  

### Output
- % active vs idle vessels  
- Trends over time  

### Business Value
- Macro trade activity indicator  
- Fleet utilization insights  
- Economic signal for shipping demand  

---

## 5. AIS Signal Gap Detection (Anomaly Detection)

### Problem
Detect vessels that stop transmitting AIS signals unexpectedly.

### Data Used
- Timestamp  
- MMSI (vessel ID)  

### Approach
- Track last-seen timestamp per vessel  
- Identify gaps exceeding threshold (e.g., >30 minutes)  

### Output
- List of vessels with signal gaps  
- Gap duration statistics  

### Business Value
- Risk and compliance monitoring  
- Detection of suspicious behavior  
- Data quality monitoring  

---

## 6. Maritime Corridor Traffic Analysis

### Problem
Identify the most heavily used shipping corridors.

### Data Used
- Vessel trajectories (lat/lon sequences)  

### Approach
- Aggregate historical movement paths  
- Convert to spatial grid (H3/geohash)  
- Count frequency of vessel passages  

### Output
- Heatmap of shipping lanes  
- Top corridors by traffic volume  

### Business Value
- Strategic trade insights  
- Infrastructure planning  
- Route optimization  

---

# 🔗 References & Existing Implementations

## AISStream Resources
- https://aisstream.io/
- https://github.com/aisstream/example-consumers

## Example Code (Streaming + AIS)
- https://github.com/aisstream/example-consumers  
  → WebSocket ingestion examples (Python, Node.js)

- https://github.com/M0r13n/ais-decoder  
  → AIS decoding and parsing

- https://github.com/schwehr/ais  
  → AIS message parsing utilities

## Platforms Using AIS Data
- MarineTraffic (commercial platform using AIS insights)
- VesselFinder (AIS-based vessel tracking)
- FleetMon (fleet monitoring and analytics)

## Useful Libraries
- Geospatial:
  - H3 (Uber hex indexing)
  - GeoPandas
- Streaming:
  - Kafka / Spark / Flink
- Visualization:
  - Mapbox / Kepler.gl

---

# 🧠 Notes

- AISStream provides **real-time raw AIS messages only**
- All higher-level insights must be derived via:
  - Event detection
  - Geospatial processing
  - Time-series aggregation

This project demonstrates:
- Streaming data ingestion  
- Geospatial analytics  
- Stateful processing  
- Business-driven data modeling  
