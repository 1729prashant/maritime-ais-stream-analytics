# configs/aisstream_config.py

# Define the region filter (bounding box: min_lat, min_lon, max_lat, max_lon)
# This bounding box is designed to cover:
# - Left: Entire coast of West India
# - Right: East-most coast of Japan
# - Lower: Just above Australia (approximately the Equator)
# - Upper: Northern parts of Japan
REGION_FILTER = {
    "min_lat": 0.0,   # Approximately the Equator, just above Australia
    "max_lat": 45.0,  # Covers northern parts of Japan
    "min_lon": 68.0,  # West coast of India
    "max_lon": 148.0  # East-most coast of Japan
}
