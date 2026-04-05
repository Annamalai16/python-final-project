from automation_script import main


def handler(event, context):
    run_date = event.get("RunDate") if isinstance(event, dict) else None
    try:
        main(run_date_override=run_date)
        return {"statusCode": 200, "body": "Pipeline completed successfully"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
