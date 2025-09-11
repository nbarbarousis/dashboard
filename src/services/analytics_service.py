# src/services/analytics_service.py
"""
Analytics service - computes metrics and generates plots from extracted rosbag data
"""

import logging
import pickle
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
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

            metrics.tracking_latency_ms = latency_result.get('tracking_latency', [])
            if metrics.tracking_latency_ms:
                metrics.mean_tracking_latency_ms = np.mean(metrics.tracking_latency_ms)

        if data.detections_json is not None and data.tracking_json is not None:
            result = self.metrics_calculator.calculate_time_series(data.detections_json, data.tracking_json)
            
            metrics.detections_over_time = result.get('detections_over_time', [])
            metrics.detections_confidence_dist = result.get('detections_confidence_dist', [])
            metrics.avg_detection_confidence = result.get('avg_detection_confidence', 0.0)
            metrics.tracks_over_time = result.get('tracks_over_time', [])
            metrics.tracks_confidence_dist = result.get('tracks_confidence_dist', [])
            metrics.avg_track_confidence = result.get('avg_track_confidence', 0.0)
            

        if data.tracking_json:
            df_lc = self.metrics_calculator.calculate_track_lifecycles(data.tracking_json)
            metrics.track_lifecycles    = df_lc
            metrics.total_tracks        = len(df_lc)
            metrics.avg_track_lifetime  = float(df_lc['track_span'].mean()) if not df_lc.empty else 0.0

            # density histogram (20 bins 0–1)
            dens = df_lc['density'].tolist()
            hist, _ = np.histogram(dens, bins=20, range=(0,1))
            metrics.track_density_dist = hist.tolist()

        metrics.computation_time = datetime.now() - metrics.computation_time
        
        return metrics
    
    def _generate_plots(self, data: ExtractedData, 
                       metrics: AnalysisMetrics) -> AnalysisPlots:
        """
        Generate all plots from data and metrics
        """
        plots = AnalysisPlots()
        
        # Generate FPS plot
        plots.fps_figure = self.plot_generator.generate_fps_plot(metrics)

        # Generate stats plot
        plots.stats_figure = self.plot_generator.generate_stats_plot(metrics)

        # Generate latency plot (detection + tracking)
        plots.latency_figure = self.plot_generator.generate_latency_plot(metrics)

        plots.lifecycle_figure = self.plot_generator.generate_lifecycle_plot(metrics)

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
# Metrics Calculator
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
    
    def calculate_time_series(self, detections: List[Dict], tracks: List[Dict]) -> Dict:
        """
        Calculate time series data from raw JSON
        
        Returns dict with:
        - detections_over_time: List of detection counts over time
        - detections_confidence_dist: List[int] histogram of detection confidences
        - tracks_over_time: List of tracks counts over time
        - tracks_confidence_dist: List[int] histogram of track confidences
        """

        import numpy as np
        from itertools import chain

        # time‐series already extracted
        detections_over_time = [msg['num_detections'] for msg in detections]
        tracks_over_time     = [msg['num_tracked'] for msg in tracks]

        # flatten all detection scores in one go
        detection_confidences = [
            obj['score']
            for msg in detections
            for obj in msg['detections']
        ]

        # flatten all track   scores in one go
        track_confidences = [
            obj['score']
            for msg in tracks
            for obj in msg['tracked_objects']
        ]

        # average confidences (just as an example—you can drop these if you only need the hist)
        avg_det_conf = float(np.mean(detection_confidences)) if detection_confidences else 0.0
        avg_trk_conf = float(np.mean(track_confidences)) if track_confidences else 0.0

        # build a 20‐bin histogram over [0,1]
        det_hist, _ = np.histogram(detection_confidences, bins=20, range=(0,1))
        trk_hist, _ = np.histogram(track_confidences,   bins=24, range=(0,1.2))

        return {
            'detections_over_time':        detections_over_time,
            'detections_confidence_dist':  det_hist.tolist(),
            'tracks_over_time':            tracks_over_time,
            'tracks_confidence_dist':      trk_hist.tolist(),
            'avg_detection_confidence':    avg_det_conf,
            'avg_track_confidence':        avg_trk_conf
        }
    
    def calculate_track_lifecycles(self, track_full: List[Dict]) -> pd.DataFrame:
        """Compute per‐track span, density, confidence stats."""
        track_lifecycles = {}
        for msg in track_full:
            seq = msg['seq']
            for obj in msg['tracked_objects']:
                tid = obj['tracking_id']
                rec = track_lifecycles.setdefault(tid, {'confidences': [], 'sequences': [], 'count': 0})
                rec['confidences'].append(obj['score'])
                rec['sequences'].append(seq)
                rec['count'] += 1
        if not track_lifecycles:
            return pd.DataFrame()

        rows = []
        for tid, d in track_lifecycles.items():
            seqs = sorted(d['sequences'])
            active = d['count']
            span = seqs[-1] - seqs[0] + 1
            density = active / span if span>0 else 0
            confs = np.array(d['confidences'])
            rows.append({
                'track_id':        tid,
                'active_frames':   active,
                'track_span':      span,
                'density':         density,
                'avg_confidence':  confs.mean(),
                'max_confidence':  confs.max(),
                'min_confidence':  confs.min(),
                'first_seq':       seqs[0],
                'last_seq':        seqs[-1]
            })
        return pd.DataFrame(rows)


# ============================================================================
# Plot Generator
# ============================================================================

class PlotGenerator:
    """
    Generator for analysis plots.
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
    
    def generate_stats_plot(self,
                            metrics: AnalysisMetrics) -> go.Figure:
        """
        Generate detection/tracking statistics plot
        
        Returns Plotly figure as JSON dict
        """
        import numpy as np
        from plotly.subplots import make_subplots

        # 2×3 grid
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=[
                'Detection Count per Frame',
                'Detection Count Distribution',
                'Detection Confidence Distribution',
                'Track Count per Frame',
                'Track Count Distribution',
                'Track Confidence Distribution'
            ],
            specs=[
                [{'type': 'scatter'}, {'type': 'histogram'}, {'type': 'bar'}],
                [{'type': 'scatter'}, {'type': 'histogram'}, {'type': 'bar'}]
            ],
            horizontal_spacing=0.12,
            vertical_spacing=0.15
        )

        # Row 1 Col 1: Detection count over time
        det_time = metrics.detections_over_time or []
        fig.add_trace(
            go.Scatter(
                x=list(range(len(det_time))),
                y=det_time,
                mode='lines+markers',
                line=dict(color='green', width=1),
                marker=dict(size=3),
                name='Detections'
            ),
            row=1, col=1
        )
        # Row 1 Col 2: Detection count distribution
        if det_time:
            fig.add_trace(
                go.Histogram(
                    x=det_time,
                    marker_color='green',
                    opacity=0.7,
                    xbins=dict(start=0, end=max(det_time)+1, size=1),
                    name='Det Count Dist'
                ),
                row=1, col=2
            )
        # Row 1 Col 3: Detection confidence distribution
        det_conf = metrics.detections_confidence_dist or []
        if det_conf:
            bins = np.linspace(0, 1, len(det_conf)+1)
            centers = (bins[:-1] + bins[1:]) / 2
            fig.add_trace(
                go.Bar(
                    x=centers,
                    y=det_conf,
                    marker_color='green',
                    name='Det Conf Dist'
                ),
                row=1, col=3
            )

        # Row 2 Col 1: Track count over time
        trk_time = metrics.tracks_over_time or []
        fig.add_trace(
            go.Scatter(
                x=list(range(len(trk_time))),
                y=trk_time,
                mode='lines+markers',
                line=dict(color='orange', width=1),
                marker=dict(size=3),
                name='Tracks'
            ),
            row=2, col=1
        )
        # Row 2 Col 2: Track count distribution
        if trk_time:
            fig.add_trace(
                go.Histogram(
                    x=trk_time,
                    marker_color='orange',
                    opacity=0.7,
                    xbins=dict(start=0, end=max(trk_time)+1, size=1),
                    name='Track Count Dist'
                ),
                row=2, col=2
            )
        # Row 2 Col 3: Track confidence distribution
        trk_conf = metrics.tracks_confidence_dist or []
        if trk_conf:
            bins = np.linspace(0, 1.2, len(trk_conf)+1)
            centers = (bins[:-1] + bins[1:]) / 2
            fig.add_trace(
                go.Bar(
                    x=centers,
                    y=trk_conf,
                    marker_color='orange',
                    name='Track Conf Dist'
                ),
                row=2, col=3
            )

        # Axes labels
        fig.update_xaxes(title_text="Frame Index", row=1, col=1)
        fig.update_xaxes(title_text="Detections per Frame", row=1, col=2)
        fig.update_xaxes(title_text="Confidence", row=1, col=3, range=[0,1])
        fig.update_xaxes(title_text="Frame Index", row=2, col=1)
        fig.update_xaxes(title_text="Tracks per Frame", row=2, col=2)
        fig.update_xaxes(title_text="Confidence", row=2, col=3, range=[0,1.2])

        fig.update_yaxes(title_text="Count", row=1, col=1)
        fig.update_yaxes(title_text="Frequency", row=1, col=2)
        fig.update_yaxes(title_text="Frequency", row=1, col=3)
        fig.update_yaxes(title_text="Count", row=2, col=1)
        fig.update_yaxes(title_text="Frequency", row=2, col=2)
        fig.update_yaxes(title_text="Frequency", row=2, col=3)

        fig.update_layout(
            height=900,
            showlegend=False,
            margin=dict(l=60, r=20, t=60, b=60)
        )
        return fig
    
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
    
    def generate_lifecycle_plot(self, metrics: AnalysisMetrics) -> go.Figure:
        import numpy as np
        from plotly.subplots import make_subplots

        df = metrics.track_lifecycles if metrics.track_lifecycles is not None else pd.DataFrame()
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Track Span Distribution',
                'Active Frames Distribution',
                'Density Distribution',
                'Confidence vs Track Span'
            ],
            specs=[
                [{'type':'histogram'},{'type':'histogram'}],
                [{'type':'histogram'},{'type':'scatter'}]
            ],
            horizontal_spacing=0.12, vertical_spacing=0.2,
            row_heights=[0.5,0.5]
        )

        if not df.empty:
            spans   = df['track_span']
            active  = df['active_frames']
            density = df['density']
            avgc    = df['avg_confidence']

            # span hist
            fig.add_trace(
                go.Histogram(x=spans, xbins=dict(start=0,end=spans.max(),size=5),
                             marker_color='#8B4789', opacity=0.7, showlegend=False),
                row=1, col=1
            )
            fig.add_vline(x=spans.mean(), line_dash='dash', line_color='red', row=1, col=1)
            fig.add_annotation(x=spans.mean(), y=0.95, yref='y domain',
                               text=f'μ={spans.mean():.1f}', showarrow=False,
                               font=dict(color='red', size=11),
                               row=1, col=1)

            # active hist
            fig.add_trace(
                go.Histogram(x=active, xbins=dict(start=0,end=active.max(),size=5),
                             marker_color='#D2691E', opacity=0.7, showlegend=False),
                row=1, col=2
            )
            fig.add_vline(x=active.mean(), line_dash='dash', line_color='red', row=1, col=2)
            fig.add_annotation(x=active.mean(), y=0.95, yref='y domain',
                               text=f'μ={active.mean():.1f}', showarrow=False,
                               font=dict(color='red', size=11),
                               row=1, col=2)

            # density hist
            fig.add_trace(
                go.Histogram(x=density, xbins=dict(start=0,end=1.0,size=0.05),
                             marker_color='#4682B4', opacity=0.7, showlegend=False),
                row=2, col=1
            )
            fig.add_vline(x=density.mean(), line_dash='dash', line_color='red', row=2, col=1)
            fig.add_annotation(x=density.mean(), y=0.95, yref='y domain',
                               text=f'μ={density.mean():.2f}', showarrow=False,
                               font=dict(color='red', size=11),
                               row=2, col=1)
            # quality lines
            fig.add_vline(x=0.9, line_dash='dot', line_color='green', row=2, col=1)
            fig.add_vline(x=0.5, line_dash='dot', line_color='orange', row=2, col=1)

            # scatter avg conf vs span
            fig.add_trace(
                go.Scatter(
                    x=avgc, y=spans,
                    mode='markers',
                    marker=dict(
                        size=8,
                        color=density,
                        colorscale='RdYlGn',
                        cmin=0, cmax=1,
                        showscale=True,
                        colorbar=dict(title='Density')
                    ),
                    hovertemplate='AvgConf: %{x:.2f}<br>Span: %{y}<br>Density: %{marker.color:.2f}<extra></extra>',
                    showlegend=False
                ),
                row=2, col=2
            )

        # axis labels
        fig.update_xaxes(title_text='Track Span (frames)', row=1, col=1)
        fig.update_xaxes(title_text='Active Frames',    row=1, col=2)
        fig.update_xaxes(title_text='Density',          row=2, col=1)
        fig.update_xaxes(title_text='Avg Confidence',   row=2, col=2)

        fig.update_yaxes(title_text='Count', row=1, col=1)
        fig.update_yaxes(title_text='Count', row=1, col=2)
        fig.update_yaxes(title_text='Frequency', row=2, col=1)
        fig.update_yaxes(title_text='Track Span', row=2, col=2)

        fig.update_layout(height=800, margin=dict(l=60,r=60,t=80,b=60))
        return fig