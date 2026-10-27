from process_meteoblue_hub import parse_event
from process_meteoblue_hub import run_meteoblue_ingestor, run_meteoblue_retriever


def ingestor_handler(event, context):
    """
    ingestor_handler - lambda function for the Meteoblue ingestor
    """
    kwargs = parse_event(event, run_meteoblue_ingestor)

    res = run_meteoblue_ingestor(**kwargs)

    return {
        "statusCode": 200,
        "body": {
            "result": res
        }
    }


def retriever_handler(event, context):
    """
    retriever_handler - lambda function for the Meteoblue retriever
    """
    kwargs = parse_event(event, run_meteoblue_retriever)

    res = run_meteoblue_retriever(**kwargs)

    return {
        "statusCode": 200,
        "body": {
            "result": res
        }
    }


if __name__ == "__main__":
    ingestor_event = {
        "variable": "precipitation",
        "service": "basic-1h",
        "lat_range": "45.0,46.0",
        "long_range": "7.0,8.0",
        "grid_res": "1000",
        "debug": "false"
    }
    print(ingestor_handler(ingestor_event, None))

    retriever_event = {
        "variable": "precipitation",
        "lat_range": "45.0,46.0",
        "long_range": "7.0,8.0",
        "time_range": "2025-01-21T08:00:00,2025-01-22T23:00:00",
        "out_format": "tif",
        "debug": "false"
    }
    print(retriever_handler(retriever_event, None))
