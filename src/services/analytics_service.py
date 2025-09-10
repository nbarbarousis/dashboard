# src/services/analytics_service.py
"""
Analytics service - computes metrics and generates plots from extracted rosbag data
"""

import logging
import pickle
import json
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import numpy as np
import pandas as pd

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from models.data_models import (
    RunCoordinate, RunAnalysis, ExtractedData, 
    AnalysisMetrics, AnalysisPlots,
    DataStatus, ProcessingStatus
)
from services.rosbag_service import RosbagService

logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    Handles analysis of extracted rosbag data.
    Computes metrics, generates plots, and manages caching.
    """
    
    def __init__(self, rosbag_service: RosbagService, processed_root: Path, 
                 enable_caching: bool = True):
        """
        Initialize analytics service
        
        Args:
            rosbag_service: Service for loading extracted data
            processed_root: Root directory for processed directory
            enable_caching: Whether to use caching
        """
        self.rosbag_service = rosbag_service
        self.processed_root = Path(processed_root)
        self.enable_caching = enable_caching
        
        # Initialize calculators (these would be imported from refactored rosbag-analysis)
        self.metrics_calculator = MetricsCalculator()
        self.plot_generator = PlotGenerator()
    
    def analyze_run(self, coord: RunCoordinate, 
                   force_refresh: bool = False) -> RunAnalysis:
        """
        Complete analysis pipeline for a run
        
        Args:
            coord: Run coordinate
            force_refresh: Force recomputation even if cached
            
        Returns:
            Complete RunAnalysis object
        """
        # Check cache first
        if self.enable_caching and not force_refresh:
            cached = self._load_cached_analysis(coord)
            if cached:
                logger.info(f"Loaded cached analysis for {coord.timestamp}")
                return cached
        
        # Create analysis object
        analysis = RunAnalysis(
            coordinate=coord,
            status=DataStatus.NOT_DOWNLOADED,
            processing_status=ProcessingStatus.PENDING
        )
        
        # Load extracted data
        extracted_data = self.rosbag_service.load_extracted_data(coord)
        
        if not extracted_data:
            analysis.status = DataStatus.NOT_DOWNLOADED
            analysis.error_message = "No extracted data available"
            return analysis
        
        analysis.extracted_data = extracted_data
        analysis.status = DataStatus.EXTRACTED
        analysis.processing_status = ProcessingStatus.IN_PROGRESS
        
        try:
            # Compute metrics
            logger.info(f"Computing metrics for {coord.timestamp}")
            analysis.metrics = self._compute_metrics(extracted_data)
            
            # Generate plots
            logger.info(f"Generating plots for {coord.timestamp}")
            analysis.plots = self._generate_plots(extracted_data, analysis.metrics)
            
            # Update status
            analysis.status = DataStatus.ANALYZED
            analysis.processing_status = ProcessingStatus.COMPLETE
            analysis.updated_at = datetime.now()
            
            logger.info(f"Metrics and plots generated for {coord.timestamp}")
            # Cache the analysis
            if self.enable_caching:
                self._cache_analysis(analysis)
            
            logger.info(f"Analysis complete for {coord.timestamp}")
            
        except Exception as e:
            logger.error(f"Analysis failed for {coord.timestamp}: {e}")
            analysis.processing_status = ProcessingStatus.FAILED
            analysis.error_message = str(e)
        
        return analysis
    
    def _compute_metrics(self, data: ExtractedData) -> AnalysisMetrics:
        """
        Compute all metrics from extracted data
        
        Args:
            data: Estracted rosbag data
            
        Returns:
            AnalysisMetrics object
        """
        metrics = AnalysisMetrics()
        metrics.computation_time = datetime.now()
        
        # Calculate Frame FPS metrics
        if data.frames_df is not None and not data.frames_df.empty:
            fps_metrics = self.metrics_calculator.calculate_fps_metrics(data.frames_df)
            metrics.frame_fps_instant = fps_metrics.get('instant', [])
            metrics.frame_fps_rolling = fps_metrics.get('rolling', [])

        if data.detections_df is not None and not data.detections_df.empty:
            fps_metrics = self.metrics_calculator.calculate_fps_metrics(data.detections_df)
            metrics.detection_fps_instant = fps_metrics.get('instant', [])
            metrics.detection_fps_rolling = fps_metrics.get('rolling', [])

        if data.tracking_df is not None and not data.tracking_df.empty:
            fps_metrics = self.metrics_calculator.calculate_fps_metrics(data.tracking_df)
            metrics.tracking_fps_instant = fps_metrics.get('instant', [])
            metrics.tracking_fps_rolling = fps_metrics.get('rolling', [])

        # Calculate latency metrics
        if data.detections_df is not None and not data.detections_df.empty:
            latency_result = self.metrics_calculator.calculate_latencies(
                data.detections_df, data.tracking_df
            )
            metrics.detection_latency_ms = latency_result.get('detection_latency', [])
            if metrics.detection_latency_ms:
                metrics.mean_detection_latency_ms = np.mean(metrics.detection_latency_ms)
                metrics.max_detection_latency_ms = np.max(metrics.detection_latency_ms)

            metrics.tracking_latency_ms = latency_result.get('tracking_latency', [])
            if metrics.tracking_latency_ms:
                metrics.mean_tracking_latency_ms = np.mean(metrics.tracking_latency_ms)
                metrics.max_tracking_latency_ms = np.max(metrics.tracking_latency_ms)
        
        # # Calculate detection statistics
        # if data.detections_df is not None and not data.detections_df.empty:
        #     metrics.total_detections = data.detections_df['num_detections'].sum()
        #     metrics.avg_detections_per_frame = data.detections_df['num_detections'].mean()
            
        #     # Confidence distribution from JSON data
        #     if data.detections_json:
        #         confidences = []
        #         for msg in data.detections_json:
        #             for det in msg.get('detections', []):
        #                 confidences.append(det.get('score', 0))
                
        #         if confidences:
        #             metrics.detection_confidence_dist = {
        #                 'bins': np.histogram(confidences, bins=20, range=(0, 1))[0].tolist(),
        #                 'mean': np.mean(confidences),
        #                 'std': np.std(confidences)
        #             }
        
        # # Calculate tracking statistics
        # if data.tracking_df is not None and not data.tracking_df.empty:
        #     metrics.total_tracks = data.tracking_df['num_tracked'].sum()
            
        #     # Track lifecycle analysis from JSON data
        #     if data.tracking_json:
        #         track_stats = self.metrics_calculator.calculate_track_lifecycles(
        #             data.tracking_json
        #         )
        #         if track_stats:
        #             metrics.avg_track_lifetime = track_stats.get('avg_lifetime')
        #             metrics.track_density_dist = track_stats.get('density_dist')
        
        return metrics
    
    def _generate_plots(self, data: ExtractedData, 
                       metrics: AnalysisMetrics) -> AnalysisPlots:
        """
        Generate all plots from data and metrics
        """
        plots = AnalysisPlots()
        
        # Generate FPS plot
        plots.fps_figure = self.plot_generator.generate_fps_plot(metrics)

        # Generate latency plot (detection + tracking)
        plots.latency_figure = self.plot_generator.generate_latency_plot(metrics)

        return plots
    
    def _cache_analysis(self, analysis: RunAnalysis):
        """Cache analysis results to disk"""
        cache_path = self._get_analysis_cache_path(analysis.coordinate)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        analysis.cache_path = cache_path
        analysis.processing_status = ProcessingStatus.CACHED
        
        with open(cache_path, 'wb') as f:
            pickle.dump(analysis, f)
        
        logger.debug(f"Cached analysis to {cache_path}")
    
    def _load_cached_analysis(self, coord: RunCoordinate) -> Optional[RunAnalysis]:
        """Load cached analysis from disk"""
        cache_path = self._get_analysis_cache_path(coord)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'rb') as f:
                analysis = pickle.load(f)
            
            # Verify it's still valid
            if analysis.coordinate.to_dict() == coord.to_dict():
                analysis.processing_status = ProcessingStatus.CACHED
                return analysis
                
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
        
        return None
    
    def _get_analysis_cache_path(self, coord: RunCoordinate) -> Path:
        """Get cache file path for coordinate"""
        return self.processed_root / coord.to_path_str() / "analysis.pkl"


# ============================================================================
# Metrics Calculator - Skeleton (would be fully implemented from rosbag-analysis)
# ============================================================================

class MetricsCalculator:
    """
    Calculator for rosbag analysis metrics.
    """
    
    def calculate_fps_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate FPS metrics handling bag boundaries
        
        Returns dict with:
        - instant: List of instantaneous FPS values
        - rolling: List of rolling average FPS  
        """
        
        # Assert required columns exist in the dataframe
        assert 'header_timestamp_s' and 'bag_file' in df.columns, "df must contain 'header_timestamp_s', 'bag_file' columns"
        
        df = df.copy()
          
        # Calculate intervals, marking bag boundaries
        df['interval_s'] = df['header_timestamp_s'].diff()
        
        # Mark where bag changes occur
        df['bag_change'] = df['bag_file'].ne(df['bag_file'].shift())
        
        # Set interval to NaN at bag boundaries
        df.loc[df['bag_change'], 'interval_s'] = np.nan
        
        # Calculate instantaneous FPS
        df['fps_instant_raw'] = 1.0 / df['interval_s'].where(df['interval_s'] > 0)
        
        # Rolling average on the raw FPS, handling NaN values
        df['fps_rolling_25'] = df['fps_instant_raw'].rolling(window=25, min_periods=5).mean()
        
        return {
            'instant': df['fps_instant_raw'].tolist(),
            'rolling': df['fps_rolling_25'].tolist()
            }
    
    def calculate_latencies(self, detections_df: pd.DataFrame, 
                          tracking_df: pd.DataFrame) -> Dict:
        """
        Calculate processing latencies
        
        Returns dict with:
        - detection_latency: List of detection latencies in ms
        """
        # Assert required columns exist in the dataframes
        assert 'header_timestamp_s' and 'image_timestamp_s' in detections_df.columns, "detections_df must contain 'header_timestamp_s', 'image_timestamp_s' columns"
        assert 'header_timestamp_s' and 'image_timestamp_s' in tracking_df.columns, "tracking_df must contain 'header_timestamp_s', 'image_timestamp_s' columns"

        det_df = detections_df.copy()
        track_df = tracking_df.copy()
        
        # Detection latency (ms) - time from image capture to detection output
        det_df['latency_ms'] =  1000 * (det_df['header_timestamp_s'] - det_df['image_timestamp_s'])
        track_df['latency_ms'] = 1000 * (track_df['header_timestamp_s'] - track_df['image_timestamp_s'])
        
        # For now, only return detection latency as requested
        return {
            'detection_latency': det_df['latency_ms'].to_list(),
            'tracking_latency': track_df['latency_ms'].to_list()
        }
    
    # def calculate_track_lifecycles(self, tracking_json: Dict) -> pd.DataFrame:
    #     """
    #     Parse the raw tracking JSON and compute per-track lifecycles.

    #     Returns:
    #         pd.DataFrame with columns:
    #             - track_id
    #             - active_frames
    #             - track_span
    #             - density
    #             - avg_confidence
    #             - max_confidence
    #             - min_confidence
    #             - first_seq
    #             - last_seq
    #     """
    #     track_lifecycles = {}
                
    #     # First pass: collect all data
    #     for msg_idx, track_msg in enumerate(tracking_json):
    #         seq = track_msg['seq']
    #         for obj in track_msg['tracked_objects']:
    #             track_id = obj['tracking_id']
    #             if track_id not in track_lifecycles:
    #                 track_lifecycles[track_id] = {
    #                     'confidences': [],
    #                     'sequences': [],
    #                     'count': 0
    #                 }
    #             track_lifecycles[track_id]['confidences'].append(obj['score'])
    #             track_lifecycles[track_id]['sequences'].append(seq)
    #             track_lifecycles[track_id]['count'] += 1
        
    #     if not track_lifecycles:
    #         return pd.DataFrame()
        
    #     # Calculate metrics for each track
    #     results = []
    #     for tid, data in track_lifecycles.items():
    #         sequences = sorted(data['sequences'])
            
    #         # Active frames (current metric)
    #         active_frames = data['count']
            
    #         # Track span (new metric)
    #         first_seq = sequences[0]
    #         last_seq = sequences[-1]
    #         track_span = last_seq - first_seq + 1
            
    #         # Density (new metric) - ratio of active to span
    #         density = active_frames / track_span if track_span > 0 else 0
            
    #         # Confidence metrics
    #         confidences = data['confidences']
    #         avg_confidence = np.mean(confidences)
    #         max_confidence = np.max(confidences)
    #         min_confidence = np.min(confidences)
            
    #         results.append({
    #             'track_id': tid,
    #             'active_frames': active_frames,
    #             'track_span': track_span,
    #             'density': density,
    #             'avg_confidence': avg_confidence,
    #             'max_confidence': max_confidence,
    #             'min_confidence': min_confidence,
    #             'first_seq': first_seq,
    #             'last_seq': last_seq
    #         })
        
    #     return pd.DataFrame(results)


# ============================================================================
# Plot Generator - Skeleton (would be fully implemented from rosbag-analysis)
# ============================================================================

class PlotGenerator:
    """
    Generator for analysis plots.
    This is a skeleton - full implementation would come from rosbag-analysis/plotting.py
    """
    
    def generate_fps_plot(self,
                          metrics: AnalysisMetrics,
                          fps_clip: float = 60.0,
                          hline: float = 25.0
                          ) -> go.Figure:
        """
        Generate FPS analysis plot (3×3):
         - Row 1: temporal instant & rolling
         - Row 2: instant FPS distribution
         - Row 3: rolling FPS distribution

        Expects AnalysisMetrics to have:
          frame_fps_instant, frame_fps_rolling,
          detection_fps_instant, detection_fps_rolling,
          tracking_fps_instant, tracking_fps_rolling
        """
        fig = make_subplots(
            rows=3, cols=3,
            subplot_titles=[
                "Frame Over Time", "Detection Over Time", "Tracking Over Time",
                "Frame Inst. FPS Dist", "Det. Inst. FPS Dist", "Track Inst. FPS Dist",
                "Frame Rolling FPS Dist", "Det. Rolling FPS Dist", "Track Rolling FPS Dist"
            ],
            horizontal_spacing=0.08, vertical_spacing=0.12
        )
        
        sources = [
            (metrics.frame_fps_instant, metrics.frame_fps_rolling, "Frame", "blue"),
            (metrics.detection_fps_instant, metrics.detection_fps_rolling, "Detection", "green"),
            (metrics.tracking_fps_instant, metrics.tracking_fps_rolling, "Tracking", "orange"),
        ]
        
        for col, (inst, roll, name, color) in enumerate(sources, start=1):
            # Row 1: temporal
            x_inst = list(range(len(inst)))
            y_inst = [min(v, fps_clip) for v in inst]
            fig.add_trace(
                go.Scatter(x=x_inst, y=y_inst,
                           mode="lines", name=f"{name} Instant",
                           line=dict(color=color, width=0.5),
                           opacity=0.3,
                           showlegend=(col == 1)),
                row=1, col=col
            )

            x_roll = list(range(len(roll)))
            y_roll = [min(v, fps_clip) for v in roll]
            fig.add_trace(
                go.Scatter(x=x_roll, y=y_roll,
                           mode="lines", name=f"{name} Rolling",
                           line=dict(color=color, width=2),
                           showlegend=(col == 1)),
                row=1, col=col
            )
            fig.add_hline(y=hline, line_dash="dash", line_color="red", row=1, col=col)
            fig.update_xaxes(title_text="Index", row=1, col=col)
            fig.update_yaxes(title_text="FPS", row=1, col=col, range=[0, fps_clip])
            

            # Row 2: instant FPS histogram
            fig.add_trace(
                go.Histogram(x=y_inst, name=f"{name} Inst Dist",
                             marker_color=color, opacity=0.7),
                row=2, col=col
            )
            fig.update_xaxes(title_text="FPS", row=2, col=col)
            fig.update_yaxes(title_text="Count", row=2, col=col)
            
            # Row 3: rolling FPS histogram
            fig.add_trace(
                go.Histogram(x=y_roll, name=f"{name} Rolling Dist",
                             marker_color=color, opacity=0.7),
                row=3, col=col
            )
            fig.update_xaxes(title_text="FPS", row=3, col=col)
            fig.update_yaxes(title_text="Count", row=3, col=col)
        
        fig.update_layout(
            height=800,
            title_text="FPS Analysis",
            margin=dict(l=60, r=20, t=80, b=60),
            showlegend=False
        )
        return fig
    
    def generate_stats_plot(self, detections_df: pd.DataFrame,
                           tracking_df: pd.DataFrame,
                           detections_json: Dict,
                           tracking_json: Dict) -> Dict:
        """
        Generate detection/tracking statistics plot
        
        Returns Plotly figure as JSON dict
        """
        # Implementation would generate the 2x3 stats plot from rosbag-analysis
        # ... (full implementation would go here)
        return {'data': [], 'layout': {}}  # Placeholder
    
    def generate_latency_plot(self, metrics: AnalysisMetrics) -> go.Figure:
        """
        Generate latency plots for detection & tracking in a 2×2 grid:
          • Row 1 Col 1: Detection latency over time
          • Row 1 Col 2: Detection latency distribution
          • Row 2 Col 1: Tracking latency over time
          • Row 2 Col 2: Tracking latency distribution
        """
        # build subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Detection Latency Over Time", "Detection Latency Distribution",
                "Tracking Latency Over Time",  "Tracking Latency Distribution"
            ],
            horizontal_spacing=0.15, vertical_spacing=0.15
        )
        det = metrics.detection_latency_ms or []
        mean_det = metrics.mean_detection_latency_ms or 0
        trk = metrics.tracking_latency_ms   or []
        mean_trk = metrics.mean_tracking_latency_ms or 0

        # Detection series + mean
        if det:
            fig.add_trace(
                go.Scatter(x=list(range(len(det))), y=det,
                           mode="lines", name="Detection Latency",
                           line=dict(color="green", width=1)),
                row=1, col=1
            )
            fig.add_hline(
                y=mean_det, line_dash="dash", line_color="red", line_width=2,
                annotation_text=f"Mean: {mean_det:.1f} ms",
                annotation_position="right", row=1, col=1
            )
            fig.add_trace(
                go.Histogram(x=det, name="Detection Latency",
                             marker_color="green",
                             xbins=dict(start=0, end=max(det), size=10)),
                row=1, col=2
            )

        # Tracking series + mean
        if trk:
            fig.add_trace(
                go.Scatter(x=list(range(len(trk))), y=trk,
                           mode="lines", name="Tracking Latency",
                           line=dict(color="orange", width=1)),
                row=2, col=1
            )
            mean_trk = np.mean(trk)
            fig.add_hline(
                y=mean_trk, line_dash="dash", line_color="red", line_width=2,
                annotation_text=f"Mean: {mean_trk:.1f} ms",
                annotation_position="right", row=2, col=1
            )
            fig.add_trace(
                go.Histogram(x=trk, name="Tracking Latency",
                             marker_color="orange",
                             xbins=dict(start=0, end=max(trk), size=10)),
                row=2, col=2
            )

        # Label axes
        fig.update_xaxes(title_text="Index",        row=1, col=1)
        fig.update_xaxes(title_text="Latency (ms)", row=1, col=2)
        fig.update_xaxes(title_text="Index",        row=2, col=1)
        fig.update_xaxes(title_text="Latency (ms)", row=2, col=2)
        fig.update_yaxes(title_text="Latency (ms)", row=1, col=1)
        fig.update_yaxes(title_text="Count",        row=1, col=2)
        fig.update_yaxes(title_text="Latency (ms)", row=2, col=1)
        fig.update_yaxes(title_text="Count",        row=2, col=2)

        fig.update_layout(
            height=600,
            margin=dict(l=60, r=20, t=60, b=60),
            showlegend=False
        )
        return fig
    
    def generate_lifecycle_plot(self, tracking_json: Dict) -> Dict:
        """
        Generate track lifecycle plot
        
        Returns Plotly figure as JSON dict
        """
        # Implementation would generate the 2x2 lifecycle plot from rosbag-analysis
        # ... (full implementation would go here)
        return {'data': [], 'layout': {}}  # Placeholder