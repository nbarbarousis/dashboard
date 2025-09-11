#!/usr/bin/env python3
"""
ROS Bag Data Extractor
Runs inside Docker container to extract data from ROS bags.
"""

import os
import sys
import json
import yaml
from pathlib import Path
import pandas as pd
import rosbag
from datetime import datetime

class BagDataExtractor:
    def __init__(self, timestamp_dir: str):
        self.timestamp_dir = Path(f"/data/{timestamp_dir}")
        
        if not self.timestamp_dir.exists():
            raise ValueError(f"Timestamp directory not found: {self.timestamp_dir}")
            
        # Find all bag files
        self.bags = sorted(self.timestamp_dir.glob("rosbag_*.bag"))
        print(f"Found {len(self.bags)} bag files to process")
        
        if not self.bags:
            raise ValueError(f"No ROS bags found in {self.timestamp_dir}")
            
        # Data collectors
        self.frames = []
        self.detections = []
        self.tracking = []
        self.metadata = {
            'timestamp': timestamp_dir,
            'extraction_time': datetime.now().isoformat(),
            'bags': []
        }
        
    def extract_all_bags(self):
        """Process all bags in the timestamp directory"""
        for idx, bag_path in enumerate(self.bags):
            print(f"Processing bag {idx + 1}/{len(self.bags)}: {bag_path.name}")
            try:
                self.extract_single_bag(bag_path)
            except Exception as e:
                print(f"  Warning: Failed to process {bag_path.name}: {e}")
                
    def extract_single_bag(self, bag_path: Path):
        """Extract data from a single bag"""
        bag = rosbag.Bag(str(bag_path))
        bag_name = bag_path.stem
        
        # Get bag info
        info = bag.get_type_and_topic_info()
        bag_meta = {
            'name': bag_name,
            'duration': bag.get_end_time() - bag.get_start_time(),
            'message_count': bag.get_message_count(),
            'topics': list(info.topics.keys())
        }
        self.metadata['bags'].append(bag_meta)
        
        # Extract frame timestamps
        frame_count = 0
        for topic, msg, t in bag.read_messages(topics=['/sensors/triton_camera_feed/compressed']):
            self.frames.append({
                'bag_file': bag_name,
                'seq': msg.header.seq,
                'bag_timestamp_s': t.to_sec(),
                'header_timestamp_s': msg.header.stamp.to_sec(),
                'has_data': len(msg.data) > 0
            })
            frame_count += 1
            
        print(f"  - Extracted {frame_count} frames")
        
        # Extract detections
        detection_count = 0
        for topic, msg, t in bag.read_messages(topics=['/weed_detection/bboxes']):
            det_data = {
                'bag_file': bag_name,
                'seq': msg.header.seq,
                'bag_timestamp_s': t.to_sec(),
                'header_timestamp_s': msg.header.stamp.to_sec(),
                'image_timestamp_s': msg.img_stamp_secs + msg.img_stamp_nsecs / 1e9,
                'num_detections': len(msg.detections),
                'detections': []
            }
            
            # Extract detailed detection info
            for det_idx, det in enumerate(msg.detections):
                # Validate expected message structure upfront
                if not hasattr(det, 'tracking_id'):
                    raise ValueError(f"Detection message seq {msg.header.seq}, index {det_idx}: missing tracking_id")
                if not hasattr(det, 'detection_msg'):
                    raise ValueError(f"Detection message seq {msg.header.seq}, index {det_idx}: missing detection_msg")
                if not hasattr(det.detection_msg, 'results'):
                    raise ValueError(f"Detection message seq {msg.header.seq}, index {det_idx}: missing detection_msg.results")
                if not hasattr(det.detection_msg, 'bbox'):
                    raise ValueError(f"Detection message seq {msg.header.seq}, index {det_idx}: missing detection_msg.bbox")
                    
                # Extract data from the correct structure
                det_info = {
                    'tracking_id': det.tracking_id,  # Usually -1 for detections
                    'class_id': det.detection_msg.results[0].id,  # Class ID
                    'score': det.detection_msg.results[0].score,  # Confidence
                    'bbox_cx': det.detection_msg.bbox.center.x,
                    'bbox_cy': det.detection_msg.bbox.center.y,
                    'bbox_theta': det.detection_msg.bbox.center.theta,  # Include theta
                    'bbox_w': det.detection_msg.bbox.size_x,
                    'bbox_h': det.detection_msg.bbox.size_y
                }
                    
                det_data['detections'].append(det_info)
                
            self.detections.append(det_data)
            detection_count += 1
            
        print(f"  - Extracted {detection_count} detection messages")
        
        # Extract tracking
        tracking_count = 0
        for topic, msg, t in bag.read_messages(topics=['/multi_object_tracking/track_bboxes']):
            track_data = {
                'bag_file': bag_name,
                'seq': msg.header.seq,
                'bag_timestamp_s': t.to_sec(),
                'header_timestamp_s': msg.header.stamp.to_sec(),
                'image_timestamp_s': msg.img_stamp_secs + msg.img_stamp_nsecs / 1e9,
                'num_tracked': len(msg.tracked_detections),
                'tracked_objects': []
            }
            
            # Extract tracked object info
            for tracked_idx, tracked in enumerate(msg.tracked_detections):
                # Validate expected message structure upfront
                if not hasattr(tracked, 'tracking_id'):
                    raise ValueError(f"Tracking message seq {msg.header.seq}, index {tracked_idx}: missing tracking_id")
                if not hasattr(tracked, 'detection_msg'):
                    raise ValueError(f"Tracking message seq {msg.header.seq}, index {tracked_idx}: missing detection_msg")
                if not hasattr(tracked.detection_msg, 'results'):
                    raise ValueError(f"Tracking message seq {msg.header.seq}, index {tracked_idx}: missing detection_msg.results")
                if not hasattr(tracked.detection_msg, 'bbox'):
                    raise ValueError(f"Tracking message seq {msg.header.seq}, index {tracked_idx}: missing detection_msg.bbox")
                    
                # Extract data from tracking message structure
                track_info = {
                    'tracking_id': tracked.tracking_id,
                    'detection_seq': tracked.detection_msg.header.seq,  # Links to detection sequence
                    'class_id': tracked.detection_msg.results[0].id,  # Class ID
                    'score': tracked.detection_msg.results[0].score,  # Confidence
                    'bbox_cx': tracked.detection_msg.bbox.center.x,
                    'bbox_cy': tracked.detection_msg.bbox.center.y,
                    'bbox_theta': tracked.detection_msg.bbox.center.theta,  # Include theta
                    'bbox_w': tracked.detection_msg.bbox.size_x,
                    'bbox_h': tracked.detection_msg.bbox.size_y
                }
                    
                track_data['tracked_objects'].append(track_info)
                
            self.tracking.append(track_data)
            tracking_count += 1
            
        print(f"  - Extracted {tracking_count} tracking messages")
        
        bag.close()
        
    def save_results(self):
        """Save extracted data to CSV and JSON files"""
        print("\nSaving extracted data...")
        
        # Save frames CSV
        if self.frames:
            frames_df = pd.DataFrame(self.frames)
            frames_df.to_csv("/output/frames.csv", index=False)
            print(f"  - Saved {len(frames_df)} frame records to frames.csv")
        
        # Save detection summary CSV
        if self.detections:
            det_summary = []
            for d in self.detections:
                det_summary.append({
                    'bag_file': d['bag_file'],
                    'seq': d['seq'],
                    'bag_timestamp_s': d['bag_timestamp_s'],
                    'header_timestamp_s': d['header_timestamp_s'],
                    'image_timestamp_s': d['image_timestamp_s'],
                    'num_detections': d['num_detections']
                })
            pd.DataFrame(det_summary).to_csv("/output/detections.csv", index=False)
            print(f"  - Saved {len(det_summary)} detection records to detections.csv")
            
            # Save full detection data as JSON
            with open("/output/detections_full.json", 'w') as f:
                json.dump(self.detections, f, indent=2)
            print(f"  - Saved full detection data to detections_full.json")
        
        # Save tracking summary CSV
        if self.tracking:
            track_summary = []
            for t in self.tracking:
                track_summary.append({
                    'bag_file': t['bag_file'],
                    'seq': t['seq'],
                    'bag_timestamp_s': t['bag_timestamp_s'],
                    'header_timestamp_s': t['header_timestamp_s'],
                    'image_timestamp_s': t['image_timestamp_s'],
                    'num_tracked': t['num_tracked']
                })
            pd.DataFrame(track_summary).to_csv("/output/tracking.csv", index=False)
            print(f"  - Saved {len(track_summary)} tracking records to tracking.csv")
            
            # Save full tracking data as JSON
            with open("/output/tracking_full.json", 'w') as f:
                json.dump(self.tracking, f, indent=2)
            print(f"  - Saved full tracking data to tracking_full.json")
        
        # Save metadata
        with open("/output/metadata.yaml", 'w') as f:
            yaml.dump(self.metadata, f, default_flow_style=False)
        print(f"  - Saved metadata.yaml")

def main():
    try:
        timestamp = os.environ.get('TIMESTAMP')
        if not timestamp:
            raise ValueError("TIMESTAMP environment variable not set")
            
        print(f"Starting extraction for timestamp: {timestamp}")
        print("-" * 50)
        
        extractor = BagDataExtractor(timestamp)
        extractor.extract_all_bags()
        extractor.save_results()
        
        print("-" * 50)
        print("Extraction completed successfully!")
        
    except Exception as e:
        print(f"Error during extraction: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()