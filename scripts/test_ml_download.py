#!/usr/bin/env python3
"""
Test ML download operation (dry‐run by default).
"""
import sys
import logging
import argparse
from pathlib import Path

# ensure src/ is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import RunCoordinate, TransferJob, DashboardConfig
from src.services import ServiceContainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(
        description="Test ML download operation (dry‐run by default)"
    )
    p.add_argument(
        "-c", "--coordinate",
        required=True,
        help="Coordinate path: cid/regionid/fieldid/twid/lbid/timestamp"
    )
    p.add_argument(
        "-b", "--bag-names",
        nargs="+",
        help="Specific bag names to download (cloud format, e.g. _2025-08-04-10-33-23_0)"
    )
    p.add_argument(
        "-t", "--file-types",
        nargs="+",
        choices=["frames", "labels"],
        help="File types to download"
    )
    p.add_argument(
        "--all",
        action="store_true",
        help="Download all ML files for coordinate"
    )
    p.add_argument(
        "--conflict",
        choices=["skip", "overwrite"],
        default="skip",
        help="Conflict resolution strategy"
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform downloads (default is dry‐run)"
    )
    return p.parse_args()


def display_plan(plan):
    print("\nTransfer Plan Summary:")
    print(f"  Coordinate       : {plan.coordinate.to_path_str()}")
    print(f"  Files to transfer: {plan.total_files}")
    print(f"  Total size       : {plan.total_size / (1024*1024):.2f} MB")
    print(f"  Files to skip    : {len(plan.files_to_skip)}")
    print(f"  Conflicts        : {len(plan.conflicts)}")
    if plan.files_to_transfer:
        print("\nFiles to Download:")
        for f in plan.files_to_transfer[:10]:
            mb = f["size"] / (1024*1024)
            conflict_str = " (conflict)" if f.get("conflict") else ""
            print(f"  • {f['filename']} ({f['file_type']}) from bag {f['cloud_bag_name']} ({mb:.2f} MB){conflict_str}")
        if len(plan.files_to_transfer) > 10:
            print(f"  ... and {len(plan.files_to_transfer) - 10} more files")
    if plan.files_to_skip:
        print("\nFiles to Skip:")
        for f in plan.files_to_skip[:10]:
            mb = f["size"] / (1024*1024)
            print(f"  • {f['filename']} ({f['file_type']}): {f['reason']} ({mb:.2f} MB)")
        if len(plan.files_to_skip) > 10:
            print(f"  ... and {len(plan.files_to_skip) - 10} more files")    
    if plan.conflicts:
        print("\nConflicts:")
        for c in plan.conflict[:10]:
            print(f"  • {c['filename']} ({c['file_type']}): {c['reason']}")
            print(f"    Cloud: {c.get('cloud_size')}B, Local: {c.get('local_size')}B")
        if len(plan.conflicts) > 10:
            print(f"  ... and {len(plan.conflicts) - 10} more conflicts")

def main():
    args = parse_args()

    # parse coordinate
    parts = args.coordinate.split("/")
    if len(parts) != 6:
        print("Error: coordinate must have 6 parts (cid/regionid/fieldid/twid/lbid/timestamp)")
        sys.exit(1)
    coord = RunCoordinate(*parts)

    # Build selection criteria
    selection_criteria = {}
    if args.all:
        selection_criteria["all"] = True
    elif args.bag_names:
        selection_criteria["bag_names"] = args.bag_names
    else:
        print("Error: must specify --bag-names or --all")
        sys.exit(1)
    if args.file_types:
        selection_criteria["file_types"] = args.file_types

    # load config
    config = DashboardConfig(
        raw_data_root=Path("/home/nikbarb/data-annot-pipeline/data/raw"),
        processed_data_root=Path("/home/nikbarb/data-annot-pipeline/data/processed"),
        ml_data_root=Path("/home/nikbarb/data-annot-pipeline/data/ML"),
        cache_root=Path("data/cache"),
        raw_bucket_name="terra-weeder-deployments-data-raw",
        processed_bucket_name="terra-weeder-deployments-data-processed",
        ml_bucket_name="terra-weeder-deployments-data-ml",
        extraction_docker_image="rosbag-extractor",
        expected_samples_per_bag=17
    )

    print("Initializing services…")
    services = ServiceContainer.initialize(config)
    services.warm_up()

    # Check cloud status first
    print(f"\nChecking cloud ML status for {coord.to_path_str()}…")
    cloud_status = services.cloud_inventory.get_ml_status(coord)
    if not cloud_status.exists:
        print("Error: No ML data found in cloud for this coordinate")
        sys.exit(1)
    print(f"Found {cloud_status.total_samples} samples across {len(cloud_status.bag_files)} bags")

    # Check local status
    print(f"\nChecking local ML status…")
    local_status = services.local_state.get_ml_status(coord)
    if local_status.downloaded:
        print(f"Local already has {local_status.total_samples} samples in {len(local_status.bag_files)} bags")
    else:
        print("No local ML data found (will download)")

    # Create transfer job
    job = TransferJob(
        coordinate=coord,
        operation_type="ml_download",
        selection_criteria=selection_criteria,
        conflict_resolution=args.conflict,
        dry_run=not args.execute
    )

    print(f"\nExecuting {'dry‐run' if job.dry_run else 'live'} download…")
    result = services.cloud_operations.execute_transfer_job(job)

    if not result.success:
        print(f"Operation failed: {result.error}")
        sys.exit(1)

    if job.dry_run:
        display_plan(result.result["plan"])
        print("\nDRY RUN complete – no files were transferred.")
        print("Add --execute to perform actual downloads.")
    else:
        summary = result.result.get("summary", {})
        print("\nDownload Complete!")
        print(f"  Successful: {summary.get('successful', 0)} files")
        print(f"  Failed    : {summary.get('failed', 0)} files")
        print(f"  Total MB  : {summary.get('total_bytes', 0)/(1024*1024):.2f}")
        print(f"  Duration  : {summary.get('total_duration', 0):.1f}s")
        print(f"  Speed MB/s: {summary.get('average_speed_mbps', 0):.1f}")

    print("\nOperation completed successfully!")


if __name__ == "__main__":
    main()