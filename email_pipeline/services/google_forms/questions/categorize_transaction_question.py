# Request body to add a multiple-choice question

def generate_transaction_categorization_question(
    record_id,
    message_id,
    transaction_type,
    transaction_date,
    description,
    amount,
    category_ml
):

    CATEGORIZE_TRANSACTION_QUESTION = {
        "requests": [
            {
                "createItem": {
                    "item": {
                        "title": (
                            f"A transaction was recorded with the following details. "
                            f"It was automatically assigned to the following category: \"{category_ml}\". "
                            f"If this is inaccurate, please assign an appropriate category.\t"
                            f"Record ID: \"{record_id}\"; "
                            f"Message ID: \"{message_id}\"; "
                            f"Transaction Type: \"{transaction_type}\"; "
                            f"Transaction Date: \"{transaction_date}\"; "
                            f"Description: \"{description}\"; "
                            f"Amount: \"{amount}\""
                        ),
                        "questionItem": {
                            "question": {
                                "required": True,
                                "choiceQuestion": {
                                    "type": "RADIO",
                                    "options": [
                                        {"value": "Living Expenses"},
                                        {"value": "Food"},
                                        {"value": "Transportation"},
                                        {"value": "Healthcare"},
                                        {"value": "Savings & Investments"},
                                        {"value": "Entertainment"},
                                        {"value": "Education"},
                                        {"value": "Travel"},
                                        {"value": "Personal & Miscellaneous"},
                                    ],
                                    "shuffle": True,
                                },
                            }
                        },
                    },
                    "location": {"index": 0},
                }
            }
        ]
    }

    return CATEGORIZE_TRANSACTION_QUESTION