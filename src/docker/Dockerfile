FROM ros:noetic-ros-core

# Install Python dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-rosbag \
    python3-yaml \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install pandas

# Copy extraction script
COPY extract_rosbag_data.py /app/extract_rosbag_data.py

WORKDIR /app

# Run extraction script
CMD ["python3", "/app/extract_rosbag_data.py"]