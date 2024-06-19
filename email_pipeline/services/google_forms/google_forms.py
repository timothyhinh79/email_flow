from apiclient import discovery

def create_google_form(
    google_creds,
    google_form_title,
    google_form_document_title,
    google_form_questions,
):

    DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

    form_service = discovery.build(
        "forms",
        "v1",
        credentials=google_creds,
        discoveryServiceUrl=DISCOVERY_DOC,
        static_discovery=False,
    )

    # Request body for creating a form
    NEW_FORM = {
        "info": {
            "title": google_form_title,
            "documentTitle": google_form_document_title
        }
    }

    # Creates the initial form
    result = form_service.forms().create(body=NEW_FORM).execute()

    # Adds the question to the form
    question_setting = (
        form_service.forms()
        .batchUpdate(formId=result["formId"], body=google_form_questions)
        .execute()
    )

    # Prints the result to show the question has been added
    get_result = form_service.forms().get(formId=result["formId"]).execute()
    print(get_result)

    return get_result


def create_google_form_watch(google_creds, form_id, event_type, topic_name):

    request = { 
        "watch": {
            "eventType": event_type, # Required. Which event type to watch for.
            "target": {
                "topic": {
                    "topicName": topic_name, # Required. A fully qualified Pub/Sub topic name to publish the events to. This topic must be owned by the calling project and already exist in Pub/Sub.
                },
            },
        },
    }

    DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"

    form_service = discovery.build(
        "forms",
        "v1",
        credentials=google_creds,
        discoveryServiceUrl=DISCOVERY_DOC,
        static_discovery=False,
    )

    # Creates the initial form
    result = form_service.forms().watches().create(formId=form_id, body=request).execute()

    return result

