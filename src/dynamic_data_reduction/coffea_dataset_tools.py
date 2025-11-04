#!/usr/bin/env python3
"""
Coffea dataset tools for dynamic data reduction.
"""

from typing import Dict, Any, Optional, List
import ndcctools.taskvine as vine
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)


def count_events(file_path: str, tree_name: str = "Events") -> Optional[int]:
    """
    Count events in a ROOT file.

    Args:
        file_path: Path to the ROOT file
        tree_name: Name of the tree to count events in

    Returns:
        Number of events or None if failed
    """
    import uproot

    try:
        # Open file and count events
        with uproot.open(file_path) as file:
            if tree_name not in file:
                return None
            tree = file[tree_name]
            num_entries = tree.num_entries
            return num_entries

    except Exception as e:
        return None


def count_events_in_files(
    file_paths: List[str], tree_name: str = "Events", timeout: int = 60
) -> List[Optional[int]]:
    """
    Call count_events_task on a list of files concurrently.

    Args:
        file_paths: List of file paths to process.
        tree_name: The tree name to count events in.
        timeout: Timeout per file in seconds.

    Returns:
        List of event counts (or None if failed) in the order of file_paths.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = [None] * len(file_paths)
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(count_events, file_path, tree_name): idx
            for idx, file_path in enumerate(file_paths)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception:
                results[idx] = None
    return results


def create_counting_task(
    file_paths: List[str], tree_name: str, timeout: int
) -> vine.Task:
    task = vine.PythonTask(count_events_in_files, file_paths, tree_name, timeout)

    # Set task properties
    task.set_cores(1)
    task.set_time_max(2 * timeout)  # Longer than our timeout as a fallback
    task.set_retries(2)

    return task


def _normalize_files_format(files):
    """Convert list format to dict format for consistent handling."""
    if isinstance(files, list):
        return {file_path: {} for file_path in files}
    return files


def _initialize_result_data(data):
    """Initialize result data structure with metadata."""
    result_data = {}
    for dataset_name, dataset_info in data.items():
        result_data[dataset_name] = {
            "files": {},
            "metadata": dataset_info.get("metadata", {}),
        }
    return result_data


def _update_progress_description(
    progress,
    main_task,
    submitted_files_count,
    completed_files_count,
    total_files_count,
    batch_task=None,
    submitted_batches_count=0,
    completed_batches=0,
    total_batches=0,
):
    """Update progress bar with current status."""
    if not progress:
        return

    progress.update(
        main_task,
        description=f"Preprocessing files: {completed_files_count} completed, {submitted_files_count} submitted",
        total=total_files_count,
        completed=completed_files_count,
    )
    progress.update(
        batch_task,
        description=f"Preprocessing batches: {completed_batches} completed, {submitted_batches_count} submitted",
        total=total_batches,
        completed=completed_batches,
    )


def _process_file_with_timeout(file_path, tree_name, timeout):
    """Process a single file with timeout handling."""
    import uproot
    import signal

    def timeout_handler(signum, frame):
        raise TimeoutError(f"Timeout after {timeout} seconds")

    try:
        # Set up timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)

        # Count events
        with uproot.open(file_path) as file:
            if tree_name in file:
                tree = file[tree_name]
                return tree.num_entries
            else:
                return None

    except Exception:
        return None
    finally:
        signal.alarm(0)  # Cancel the alarm


def preprocess(
    manager: vine.Manager,
    data: Dict[str, Any],
    tree_name: str = "Events",
    timeout: int = 120,
    max_retries: int = 3,
    show_progress: bool = True,
    batch_size: int = 5,
) -> Dict[str, Any]:
    """
    Preprocess data by counting events in ROOT files using TaskVine.

    Args:
        manager: TaskVine manager instance
        data: Input data structure with datasets and files
        tree_name: Name of the tree to count events in (default: "Events")
        timeout: Timeout in seconds for each file operation (default: 60)
        max_retries: Maximum number of retries for failed files (default: 3)
        show_progress: Whether to show progress bar (default: True)
        batch_size: Number of files to submit in initial batches (default: 5)

    Returns:
        Preprocessed data structure with num_entries added to each file
    """

    # Initialize result data structure
    result_data = _initialize_result_data(data)

    # Collect all files that need processing
    files_to_process = []

    for dataset_name, dataset_info in data.items():
        # Handle both list and dict formats for files
        files = _normalize_files_format(dataset_info.get("files", {}))

        for file_path, file_info in files.items():
            # Check if num_entries already exists
            if "num_entries" in file_info and file_info["num_entries"] is not None:
                # Already has num_entries, copy it
                result_data[dataset_name]["files"][file_path] = file_info.copy()
            else:
                # Need to count events - initialize in result_data and add to process queue
                result_data[dataset_name]["files"][file_path] = file_info.copy()
                result_data[dataset_name]["files"][file_path]["num_entries"] = None
                files_to_process.append((dataset_name, file_path, 0))

    if not files_to_process:
        return result_data

    # Set up progress bars
    progress = None
    if show_progress:
        progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=None,
            transient=False,
        )
        progress.start()
        main_task = progress.add_task(
            "Preprocessing files", total=len(files_to_process)
        )

        # Calculate total number of batches
        total_batches = (len(files_to_process) + batch_size - 1) // batch_size
        batch_task = progress.add_task("Preprocessing batches", total=total_batches)

    # Initialize task tracking
    submitted_tasks = {}  # task_id -> list of (dataset_name, file_path, retry_count)
    files_to_process_queue = (
        files_to_process.copy()
    )  # Files that still need to be processed
    completed_files_count = 0
    completed_batches = 0
    failed_files = []  # Files that failed after all retries

    # Main processing loop
    while files_to_process_queue or submitted_tasks:
        # Submit batch of files (including retries)
        if (
            files_to_process_queue and len(submitted_tasks) < 10
        ):  # Limit concurrent tasks
            batch_to_submit = files_to_process_queue[:batch_size]
            files_to_process_queue = files_to_process_queue[batch_size:]

            # Create task to count events for the entire batch
            file_paths = [fp for _, fp, _ in batch_to_submit]
            task = create_counting_task(file_paths, tree_name, timeout)

            # Submit task
            task_id = manager.submit(task)
            # Store all files in the batch with the same task_id
            submitted_tasks[task_id] = batch_to_submit

            # Update progress bar with current status
            if show_progress:
                _update_progress_description(
                    progress=progress,
                    main_task=main_task,
                    submitted_files_count=len(submitted_tasks),
                    total_files_count=len(files_to_process),
                    completed_files_count=completed_files_count,
                    batch_task=batch_task,
                    submitted_batches_count=len(submitted_tasks),
                    completed_batches=completed_batches,
                    total_batches=total_batches,
                )

        # Wait for a task to complete
        task = manager.wait(5)

        if task is None:
            continue

        if task.id not in submitted_tasks:
            continue

        batch_files = submitted_tasks[task_id]
        to_resubmit = []
        if task.successful():
            # Task completed successfully
            try:
                results = task.output  # This should be a list of results, one per file
                for i, (dataset_name, file_path, retry_count) in enumerate(batch_files):
                    if results[i] is not None:
                        num_entries = results[i]
                        result_data[dataset_name]["files"][file_path][
                            "num_entries"
                        ] = num_entries
                        completed_files_count += 1
                        # Only advance progress for successful completions
                        if progress:
                            progress.update(main_task, advance=1)
                    else:
                        to_resubmit.append((dataset_name, file_path, retry_count))

            except Exception as e:
                to_resubmit = batch_files
        else:
            to_resubmit = batch_files

        # retry all files in to_resubmit
        for dataset_name, file_path, retry_count in to_resubmit:
            if retry_count <= max_retries:
                if show_progress:
                    print(
                        f"Retrying {file_path} (attempt {retry_count + 2}/{max_retries})"
                    )
                files_to_process_queue.append(
                    (dataset_name, file_path, retry_count + 1)
                )
            else:
                failed_files.append((dataset_name, file_path))
                # Only advance progress for permanent failures
                if progress:
                    progress.update(main_task, advance=1)

        # Remove from submitted tasks
        del submitted_tasks[task_id]

        # Advance batch progress bar
        completed_batches += 1
        if show_progress:
            progress.update(batch_task, advance=1)

        # Update progress bar with current status
        if show_progress:
            _update_progress_description(
                progress=progress,
                main_task=main_task,
                submitted_files_count=len(submitted_tasks),
                total_files_count=len(files_to_process),
                completed_files_count=completed_files_count,
                batch_task=batch_task,
                submitted_batches_count=len(submitted_tasks),
                completed_batches=completed_batches,
                total_batches=total_batches,
            )

    # Finalize progress bar
    if progress:
        progress.stop()

    # Print summary
    total_files = len(files_to_process)
    successful_files = completed_files_count
    failed_count = len(failed_files)

    print(f"\nPreprocessing complete:")
    print(f"  Total files: {total_files}")
    print(f"  Successful: {successful_files}")
    print(f"  Failed: {failed_count}")
    print(f"  Batches processed: {completed_batches} / {total_batches}")

    if failed_count > 0:
        print(f"  Failed files:")
        for dataset_name, file_path in failed_files:
            print(f"    {dataset_name}: {file_path}")

    return result_data
