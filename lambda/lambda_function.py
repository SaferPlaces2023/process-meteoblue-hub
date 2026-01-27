from process_meteoblue_hub import parse_event
from process_meteoblue_hub import run_meteoblue_ingestor as main_function


def lambda_handler(event, context):
    """
    lambda_handler - lambda function for Meteoblue Ingestor
    """
    kwargs = parse_event(event, main_function)

    res = main_function(**kwargs)

    return {
        "statusCode": 200, 
        "body": {
            "result": res   
        }
    }


if __name__ == "__main__":
    event = {
        "variable": "precipitation",
        "service": "basic-1h",
        "lat_range": "45.0,46.0",
        "long_range": "7.0,8.0",
        "grid_res": "1000",
        "debug": "false"
    }

    kwargs = parse_event(event, main_function)
    res = main_function(**kwargs)
    print(res)
