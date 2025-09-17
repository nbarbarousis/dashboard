#!/usr/bin/env python3
"""
Test cloud operations with real GCS data using argparse (no click/rich).
"""
import sys
import logging
import argparse
from pathlib import Path

# ensure src/ is on PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import RunCoordinate, TransferJob, DashboardConfig
from src.services import ServiceContainer
from src.services.cloud_operations.cloud_operations_service import CloudOperationService

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(
        description="Test raw download operation against GCS (dry-run by default)"
    )
    p.add_argument(
        "-c", "--coordinate",
        required=True,
        help="Coordinate path: cid/regionid/fieldid/twid/lbid/timestamp"
    )
    p.add_argument(
        "-i", "--indices",
        nargs="+",
        type=int,
        help="Bag indices to download (e.g. -i 0 2 5)"
    )
    p.add_argument(
        "--all",
        dest="download_all",
        action="store_true",
        help="Download all bags"
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
        help="Actually perform downloads (default is dry-run)"
    )
    return p.parse_args()


def display_plan(plan):
    print("\nTransfer Plan Summary:")
    print(f"  Coordinate       : {plan.coordinate.to_path_str()}")
    print(f"  Files to transfer: {plan.total_files}")
    print(f"  Total size       : {plan.total_size / (1024*1024):.2f} MB")
    print(f"  Conflicts        : {len(plan.conflicts)}")
    if plan.files_to_transfer:
        print("\nFiles to Download:")
        for f in plan.files_to_transfer:
            mb = f["size"] / (1024*1024)
            print(f"  • {f['cloud_name']} → {f['local_name']} ({mb:.2f} MB)")
    if plan.conflicts:
        print("\nConflicts:")
        for c in plan.conflicts:
            print(f"  • {c['name']}: {c['reason']}")


def main():
    args = parse_args()

    # parse coordinate
    parts = args.coordinate.split("/")
    # if len(parts) != 6:
    #     print("Error: coordinate must have 6 parts (cid/regionid/fieldid/twid/lbid/timestamp)")
    #     sys.exit(1)

    coord = RunCoordinate(*parts)

    # load config (adjust paths as needed)
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

    op_service = CloudOperationService(
        services.cloud_inventory,
        services.local_state,
        services.path_builder
    )

    # selection criteria
    if args.download_all:
        criteria = {"all": True}
    elif args.indices:
        criteria = {"bag_indices": args.indices}
    else:
        print("Error: must specify --indices or --all")
        sys.exit(1)

    job = TransferJob(
        coordinate=coord,
        operation_type="raw_download",
        selection_criteria=criteria,
        conflict_resolution=args.conflict,
        dry_run=not args.execute
    )

    print(f"\nExecuting {'dry-run' if job.dry_run else 'live'} download…")
    result = op_service.execute_transfer_job(job)

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
        print(f"  Successful: {summary.get('successful',0)} files")
        print(f"  Failed    : {summary.get('failed',0)} files")
        print(f"  Total MB  : {summary.get('total_bytes',0)/(1024*1024):.2f}")
        print(f"  Duration  : {summary.get('total_duration',0):.1f}s")
        print(f"  Speed MB/s: {summary.get('average_speed_mbps',0):.1f}")

if __name__ == "__main__":
    main()