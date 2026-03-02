import os

from dotenv import load_dotenv
from pathlib import Path
import json
import statistics
from typing import Optional, Tuple, Dict
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def get_clean_data(unclean_data : dict) -> Tuple[dict, dict]:
    clean_time_data = {}
    clean_cost_data = {}
    for query, result in unclean_data.items():
        time_entry = {}
        cost_entry = {}
        for annotation_type, result_content in result.items():
            if annotation_type == "run_info":
                continue
            time_entry[annotation_type] = statistics.mean(result_content['time'])
            cost_entry[annotation_type] = statistics.mean(result_content['estimated_costs'])

        clean_time_data[query] = time_entry
        clean_cost_data[query] = cost_entry

    return clean_time_data, clean_cost_data

def plot_collection(data_base_dir : Path, tree_family : str, save_dir : Optional[Path]=None):
    time_dict = {}
    cost_dict = {}
    for path in Path(data_base_dir).iterdir():
        if tree_family in path.parts[-1]:
            size_key = int(path.parts[-1].split("_")[-1][:-5])
            # print(size_key)

            with open(path) as json_file:
                data = json.load(json_file)

            clean_time_data, clean_cost_data = get_clean_data(data)

            time_dict[size_key] = clean_time_data
            cost_dict[size_key] = clean_cost_data

    # print(time_dict)
    # print(cost_dict)

    plot_scalability_comparison(
        dict1=time_dict,
        dict2=cost_dict,
        dict1_title="Time vs. Annotation Strategy",
        dict2_title="Est. Cost vs. Annotation Strategy",
        overall_title=tree_family,
        log_scale=(True, True),
        save_path=save_dir / Path(f"{tree_family}.png")
    )
    # plt.show()


def plot_scalability_comparison(
        dict1: Dict[int, Dict[str, Dict[str, float]]],
        dict2: Dict[int, Dict[str, Dict[str, float]]],
        dict1_title: str = "Metric 1",
        dict2_title: str = "Metric 2",
        dict1_ylabel: str = "Time (seconds)",
        dict2_ylabel: str = "Memory (MB)",
        xlabel: str = "Tree Size (nodes)",
        overall_title: Optional[str] = None,
        figsize: Optional[Tuple[float, float]] = None,
        colors: Optional[Dict[str, str]] = None,
        line_styles: Optional[Dict[str, str]] = None,
        markers: Optional[Dict[str, str]] = None,
        log_scale: Optional[Tuple[bool, bool]] = None,
        log_scale_x: bool = True,
        save_path: Optional[Path] = None,
        dpi: int = 300,
        legend_location: str = 'best',
        grid: bool = True
) -> plt.Figure:
    """
    Create scalability visualization showing how metrics change across different tree sizes.

    Parameters:
    -----------
    dict1 : Dict[int, Dict[str, Dict[str, float]]]
        First nested dictionary {size: {query: {implementation: value}}}
    dict2 : Dict[int, Dict[str, Dict[str, float]]]
        Second nested dictionary with same structure
    dict1_title : str
        Title for the first column of plots
    dict2_title : str
        Title for the second column of plots
    dict1_ylabel : str
        Y-axis label for the first column
    dict2_ylabel : str
        Y-axis label for the second column
    xlabel : str
        X-axis label (tree sizes)
    overall_title : Optional[str]
        Overall figure title
    figsize : Optional[Tuple[float, float]]
        Figure size (width, height). If None, automatically calculated
    colors : Optional[Dict[str, str]]
        Dictionary mapping implementation names to colors
    line_styles : Optional[Dict[str, str]]
        Dictionary mapping implementation names to line styles
    markers : Optional[Dict[str, str]]
        Dictionary mapping implementation names to marker styles
    log_scale : Optional[Tuple[bool, bool]]
        Tuple of (use_log_dict1, use_log_dict2) for logarithmic y-axes
    log_scale_x : bool
        Whether to use logarithmic scale for x-axis (tree sizes)
    save_path : Optional[str]
        Path to save the figure
    dpi : int
        DPI for saved figure
    legend_location : str
        Location for legend ('best', 'upper left', etc.)
    grid : bool
        Whether to show grid lines

    Returns:
    --------
    plt.Figure
        The created matplotlib figure
    """
    # Get tree sizes (x-axis values) and sort them
    sizes = sorted(dict1.keys())

    # Get queries from first size
    queries = list(dict1[sizes[0]].keys())

    # Verify structure consistency
    assert set(sizes) == set(dict2.keys()), "Dictionaries must have same size keys"
    for size in sizes:
        assert set(queries) == set(dict1[size].keys()), f"Queries mismatch at size {size}"
        assert set(queries) == set(dict2[size].keys()), f"Queries mismatch at size {size}"

    # Get implementation names
    implementations = list(dict1[sizes[0]][queries[0]].keys())

    # Default colors
    if colors is None:
        colors = {
            'baseline': '#e74c3c',  # Red
            'dewey': '#3498db',  # Blue
            'prepost': '#2ecc71'  # Green
        }

    # Default line styles
    if line_styles is None:
        line_styles = {
            'baseline': '-',
            'dewey': '--',
            'prepost': '-.'
        }

    # Default markers
    if markers is None:
        markers = {
            'baseline': 'o',
            'dewey': 's',
            'prepost': '^'
        }

    # Default log scale settings
    if log_scale is None:
        log_scale = (False, False)

    # Calculate figure size if not provided
    if figsize is None:
        figsize = (14, 3.5 * len(queries))

    # Create subplots
    fig, axes = plt.subplots(len(queries), 2, figsize=figsize)

    # Handle case where there's only one query
    if len(queries) == 1:
        axes = axes.reshape(1, -1)

    # Set overall title if provided
    if overall_title:
        fig.suptitle(overall_title, fontsize=16, fontweight='bold', y=0.995)

    # Plot each query
    for idx, query in enumerate(queries):
        # Prepare data for dict1 (left column)
        dict1_data = {impl: [] for impl in implementations}
        for size in sizes:
            for impl in implementations:
                dict1_data[impl].append(dict1[size][query][impl])

        # Prepare data for dict2 (right column)
        dict2_data = {impl: [] for impl in implementations}
        for size in sizes:
            for impl in implementations:
                dict2_data[impl].append(dict2[size][query][impl])

        # Plot dict1 (left column)
        ax1 = axes[idx, 0]
        for impl in implementations:
            ax1.plot(
                sizes,
                dict1_data[impl],
                label=impl,
                color=colors[impl],
                linestyle=line_styles[impl],
                marker=markers[impl],
                markersize=8,
                linewidth=2.5,
                alpha=0.8
            )

        # Customize left column
        ax1.set_ylabel(dict1_ylabel, fontsize=11, fontweight='bold')
        ax1.set_xlabel(xlabel, fontsize=10)
        ax1.set_title(f"{query}", fontsize=12, fontweight='bold', pad=10)

        if log_scale_x:
            ax1.set_xscale('log')
        if log_scale[0]:
            ax1.set_yscale('log')

        if grid:
            ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)

        ax1.legend(loc=legend_location, fontsize=9, framealpha=0.9)

        # Plot dict2 (right column)
        ax2 = axes[idx, 1]
        for impl in implementations:
            ax2.plot(
                sizes,
                dict2_data[impl],
                label=impl,
                color=colors[impl],
                linestyle=line_styles[impl],
                marker=markers[impl],
                markersize=8,
                linewidth=2.5,
                alpha=0.8
            )

        # Customize right column
        ax2.set_ylabel(dict2_ylabel, fontsize=11, fontweight='bold')
        ax2.set_xlabel(xlabel, fontsize=10)
        ax2.set_title(f"{query}", fontsize=12, fontweight='bold', pad=10)

        if log_scale_x:
            ax2.set_xscale('log')
        if log_scale[1]:
            ax2.set_yscale('log')

        if grid:
            ax2.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)

        ax2.legend(loc=legend_location, fontsize=9, framealpha=0.9)

    # Add column titles
    axes[0, 0].text(
        0.5, 1.15, dict1_title,
        transform=axes[0, 0].transAxes,
        fontsize=14,
        fontweight='bold',
        ha='center'
    )
    axes[0, 1].text(
        0.5, 1.15, dict2_title,
        transform=axes[0, 1].transAxes,
        fontsize=14,
        fontweight='bold',
        ha='center'
    )

    # Adjust layout
    plt.tight_layout()

    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=dpi, bbox_inches='tight')
        print(f"Figure saved to {save_path}")

    return fig


def plot_singular(tree_name : Path, data_base_dir : Path, save_dir : Optional[Path]):
    with open(data_base_dir / tree_name) as json_file:
        data = json.load(json_file)

    # print("here")

    time_clean_data, cost_clean_data = get_clean_data(data)

    plot_benchmark_comparison(
        time_clean_data,
        cost_clean_data,
        "Time",
        "Estimated Cost",
        overall_title=tree_name.parts[-1],
        save_path=save_dir / Path(f"{tree_name.parts[-1][:-5]}.png")
    )

    # plt.show()

    # plot_grouped_bars(clean_data)


def plot_benchmark_comparison(
        dict1: Dict[str, Dict[str, float]],
        dict2: Dict[str, Dict[str, float]],
        dict1_title: str = "Metric 1",
        dict2_title: str = "Metric 2",
        dict1_ylabel: str = "Time (seconds)",
        dict2_ylabel: str = "Memory (MB)",
        overall_title: Optional[str] = None,
        figsize: Optional[Tuple[float, float]] = None,
        bar_width: float = 0.25,
        colors: Optional[Dict[str, str]] = None,
        log_scale: Optional[Tuple[bool, bool]] = None,
        save_path: Optional[Path] = None,
        dpi: int = 300
) -> plt.Figure:
    """
    Create a comparative visualization of benchmark results across different implementations.

    Parameters:
    -----------
    dict1 : Dict[str, Dict[str, float]]
        First dictionary containing benchmark results (e.g., execution time)
    dict2 : Dict[str, Dict[str, float]]
        Second dictionary containing benchmark results (e.g., memory usage)
    dict1_title : str
        Title for the first column of plots
    dict2_title : str
        Title for the second column of plots
    dict1_ylabel : str
        Y-axis label for the first column
    dict2_ylabel : str
        Y-axis label for the second column
    overall_title : Optional[str]
        Overall figure title
    figsize : Optional[Tuple[float, float]]
        Figure size (width, height). If None, automatically calculated
    bar_width : float
        Width of individual bars
    colors : Optional[Dict[str, str]]
        Dictionary mapping implementation names to colors
    log_scale : Optional[Tuple[bool, bool]]
        Tuple of (use_log_dict1, use_log_dict2) for logarithmic y-axes
    save_path : Optional[str]
        Path to save the figure. If None, figure is not saved
    dpi : int
        DPI for saved figure

    Returns:
    --------
    plt.Figure
        The created matplotlib figure
    """
    # Verify both dictionaries have the same queries
    queries = list(dict1.keys())
    assert set(queries) == set(dict2.keys()), "Dictionaries must have the same query keys"

    # Get implementation names (assuming all queries have same implementations)
    implementations = list(dict1[queries[0]].keys())

    # Default colors
    if colors is None:
        colors = {
            'baseline': '#e74c3c',  # Red
            'dewey': '#3498db',  # Blue
            'prepost': '#2ecc71'  # Green
        }

    # Default log scale settings
    if log_scale is None:
        log_scale = (False, False)

    # Calculate figure size if not provided
    if figsize is None:
        figsize = (14, 3 * len(queries))

    # Create subplots
    fig, axes = plt.subplots(len(queries), 2, figsize=figsize)

    # Handle case where there's only one query
    if len(queries) == 1:
        axes = axes.reshape(1, -1)

    # Set overall title if provided
    if overall_title:
        fig.suptitle(overall_title, fontsize=16, fontweight='bold', y=0.995)

    # X positions for bars
    x = np.arange(len(implementations))

    # Plot each query
    for idx, query in enumerate(queries):
        # Get data for this query
        dict1_values = [dict1[query][impl] for impl in implementations]
        dict2_values = [dict2[query][impl] for impl in implementations]

        # Plot dict1 (left column)
        ax1 = axes[idx, 0]
        bars1 = ax1.bar(
            x,
            dict1_values,
            bar_width * 2.5,
            color=[colors[impl] for impl in implementations],
            alpha=0.8,
            edgecolor='black',
            linewidth=1
        )

        # Customize left column
        ax1.set_ylabel(dict1_ylabel, fontsize=11, fontweight='bold')
        ax1.set_title(f"{query}", fontsize=12, fontweight='bold', pad=10)
        ax1.set_xticks(x)
        ax1.set_xticklabels(implementations, fontsize=10)
        ax1.grid(axis='y', alpha=0.3, linestyle='--')

        if log_scale[0]:
            ax1.set_yscale('log')

        # Add value labels on bars
        for bar in bars1:
            height = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2.,
                height,
                f'{height:.2f}',
                ha='center',
                va='bottom',
                fontsize=9
            )

        # Plot dict2 (right column)
        ax2 = axes[idx, 1]
        bars2 = ax2.bar(
            x,
            dict2_values,
            bar_width * 2.5,
            color=[colors[impl] for impl in implementations],
            alpha=0.8,
            edgecolor='black',
            linewidth=1
        )

        # Customize right column
        ax2.set_ylabel(dict2_ylabel, fontsize=11, fontweight='bold')
        ax2.set_title(f"{query}", fontsize=12, fontweight='bold', pad=10)
        ax2.set_xticks(x)
        ax2.set_xticklabels(implementations, fontsize=10)
        ax2.grid(axis='y', alpha=0.3, linestyle='--')

        if log_scale[1]:
            ax2.set_yscale('log')

        # Add value labels on bars
        for bar in bars2:
            height = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width() / 2.,
                height,
                f'{height:.2f}',
                ha='center',
                va='bottom',
                fontsize=9
            )

    # Add column titles
    axes[0, 0].text(
        0.5, 1.15, dict1_title,
        transform=axes[0, 0].transAxes,
        fontsize=14,
        fontweight='bold',
        ha='center'
    )
    axes[0, 1].text(
        0.5, 1.15, dict2_title,
        transform=axes[0, 1].transAxes,
        fontsize=14,
        fontweight='bold',
        ha='center'
    )

    # Adjust layout
    plt.tight_layout()

    # Save if path provided
    if save_path:
        plt.savefig(save_path, dpi=dpi, bbox_inches='tight')
        print(f"Figure saved to {save_path}")

    return fig

def plot_all_collections(res_base_dir : Path):
    for family_name in ["truebase", "ultratall", "ultrawide"]:
        plot_collection(
            data_base_dir=res_base_dir / Path("result_logs/raw_n200_filtered4"),
            tree_family=family_name,
            save_dir=res_base_dir / "visualizations3",
        )

def plot_all_singles(res_base_dir : Path):
    families=("truebase", "ultratall", "ultrawide")

    # get singles
    for path in Path(res_base_dir / Path("result_logs/raw_n200_filtered4")).iterdir():
        if not path.parts[-1].startswith(families):
            plot_singular(
                tree_name=Path(path.parts[-1]),
                save_dir=res_base_dir / "visualizations3",
                data_base_dir=res_base_dir / Path("result_logs/raw_n200_filtered4"),

            )
    pass

if __name__ == '__main__':
    load_dotenv()

    res_bd = Path(os.getenv('PROJECT_PATH')) / Path('results')

    plot_all_collections(res_bd)

    plot_all_singles(res_bd)