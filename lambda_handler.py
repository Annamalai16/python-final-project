from automation_script import main


def handler(event, context):
    try:
        main()
        return {"statusCode": 200, "body": "Pipeline completed successfully"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
