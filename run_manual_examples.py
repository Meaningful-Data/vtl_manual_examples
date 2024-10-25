import pandas as pd
import json
import csv
from pathlib import Path
from vtlengine import run
from vtlengine.API import load_datasets_with_data
from typing import Dict, Any, List, Optional, Union
from colorama import Fore, init


# Colorama initialization
init(autoreset=True)


def format_structure(structure_dict):
    for ds in structure_dict["structures"]:
        for comp in ds["components"]:
            if "TimePeriod" in comp["data_type"]:
                comp["data_type"] = comp["data_type"].replace("TimePeriod", "Time_Period")
            if "TimeInterval" in comp["data_type"]:
                comp["data_type"] = comp["data_type"].replace("TimeInterval", "Time_Interval")

    return {
        "datasets": [
            {
                "name": ds["name"],
                "DataStructure": [
                    {
                        "name": comp["name"],
                        "type": comp["data_type"],
                        "role": comp["role"],
                        "nullable": comp["role"] != "Identifier" if "nullable" not in comp.keys() else comp["nullable"],
                    }
                    for comp in ds["components"]
                ]
            }
            for ds in structure_dict["structures"]
        ]
    }


def load_json(file_path: Path):
    with file_path.open(encoding='utf-8') as f:
        return json.load(f)


def collect_datapoints(test_dir: Path, input_structure: Dict[str, Any]) -> List[Path]:
    return [test_dir / f"{ds['name']}.csv" for ds in input_structure['datasets'] if
            (test_dir / f"{ds['name']}.csv").exists()]


def run_test(test_dir: Path, operator: str):
    try:
        input_structure = format_structure(load_json(test_dir / "input.json"))
        reference_structure = format_structure(load_json(test_dir / "output.json"))

        reference_name = reference_structure['datasets'][0]['name']
        reference_data = {reference_name: pd.read_csv(test_dir / "DS_r.csv")}
        reference_datasets = load_datasets_with_data(reference_structure, reference_data)[0]

        datapoints = collect_datapoints(test_dir, input_structure)
        result = run(
            script=test_dir / "transformation.vtl",
            data_structures=input_structure,
            datapoints=datapoints,
            return_only_persistent=False
        )

        if result != reference_datasets:
            return operator, test_dir.name, "Fail", "Assertion Error"
        return operator, test_dir.name, "Ok", ""
    except Exception as e:
        error = str(e)
        if '\n' in error:
            error = error.replace('\n', ' ')
        return operator, test_dir.name, "Fail", error


def print_colored_result(operator: str, example: str, result: str, error: str):
    color = Fore.GREEN if result == "Ok" else Fore.YELLOW if result == "Not implemented" else Fore.RED
    print(f"{color}Operator: {operator}, Example: {example}, Result: {result}, Error: {error}")


def main(selected_tests: Optional[Dict[str, Union[str, List[str]]]] = None,
         not_implemented: Optional[Dict[str, Union[str, List[str]]]] = None,
         verbose: bool = False):
    base_path = Path(__file__).parent / "engine_files"
    results = []

    for operator_dir in base_path.iterdir():
        if operator_dir.is_dir():
            if selected_tests and operator_dir.name not in selected_tests:
                continue

            if not_implemented and operator_dir.name in not_implemented:
                results.extend([(operator_dir.name, test, "Not implemented", "") for test
                                in not_implemented[operator_dir.name]])

            for test_dir in operator_dir.iterdir():
                test_name = test_dir.name
                if (
                        not selected_tests or test_name in selected_tests.get(operator_dir.name, [])
                ) and (
                        not not_implemented or test_name not in not_implemented.get(operator_dir.name, [])
                ):
                    results.append(run_test(test_dir, operator_dir.name))

    if verbose:
        for result in results:
            print_colored_result(*result)

    if selected_tests is None or selected_tests == {}:
        csv_file = Path(__file__).parent / "test_result.csv"
        with csv_file.open(mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Operator", "Example", "Result", "Error"])
            writer.writerows(results)
        print(f"\n\nTests completed. Results saved in {csv_file}")

    not_implemented_tests = sum(1 for test in not_implemented.values() for _ in test)
    total_tests = len(results)
    passed_tests = sum(1 for _, _, result, _ in results if result == "Ok")
    failed_tests = total_tests - passed_tests
    success_percentage = (passed_tests * 100) // total_tests if total_tests else 0

    final_color = Fore.RED if failed_tests > 0 else Fore.YELLOW if (
            not_implemented_tests > 0) else Fore.GREEN
    print(f"{final_color}\nTotal tests: {total_tests}")
    print(f"{final_color}Passed tests: {passed_tests}")
    print(f"{final_color}Failed tests: {failed_tests}")
    print(f"{final_color}Not implemented tests: {not_implemented_tests}")
    print(f"{final_color}Success rate: {success_percentage}%")


if __name__ == "__main__":
    # Define specific tests to run. Example: {"Absolute value": ["ex1", "ex2"]}
    # If it is set to None or {}, all tests will be run
    selected_tests: Optional[Dict[str, Union[str, List[str]]]] = {}
    # Example of usage of specific_tests
    # selected_tests = {
    #     "Absolute value": ["ex_1", "ex_2"],
    #     "Case": "ex_1",
    # }

    # The tests that are defined in the not_implemented variable will not be run
    # If it is set to None or {}, all tests will be run
    # not_implemented: Optional[Dict[str, Union[str, List[str]]]] = {}
    not_implemented = {
        "Aggregate invocation": ["ex_1", "ex_2", "ex_3", "ex_4"],
        "Case": ["ex_1"],
        "Hierarchical roll-up": ["ex_1", "ex_2", "ex_3"],
        "Membership": ["ex_4", "ex_5", "ex_6"],
        "Random": ["ex_1", "ex_2"],
    }

    # Pass specific_tests to main() to run only the selected tests, default None
    # Pass verbose=True to print the results of each test in the console, default False
    main(selected_tests=selected_tests, not_implemented=not_implemented, verbose=True)