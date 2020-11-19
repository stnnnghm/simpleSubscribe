package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"os"
)

// Determine if an email exists with the given ID.
func emailExistsWithId(email string, id string) (bool, error) {
	table := os.Getenv("DB_TABLE_NAME")
	svc := dynamodb.New(session.New())
	input := &dynamodb.GetItemInput{
		// Get an item that matches email
		Key: map[string]*dynamodb.AttributeValue{
			"email": {
				S: aws.String(email),
			},
		},
		TableName: aws.String(table),
	}
	result, err := svc.GetItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Print(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Print(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				log.Print(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Print(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Print(aerr.Error())
			}
		} else {
			// Print the error, cast err awserr.Error to get the code and message
			// from an error.
			log.Print(err.Error())
		}
	}
	if result.Item == nil {
		return false, err
	}
	// Double check that the resulting email and id matches the input, return emailExistsWithId == true
	if (*result.Item["email"].S == email) && (*result.Item["id"].S == id) {
		return true, nil
	}

	log.Printf("No match for email: %s with id: %s", email, id)
	return false, err
}

// Edits an existing email's attributes. No authorization is performed here, so ensure you check that values of email and id match before calling this function
func updateItemInDynamoDB(email, id, timestamp string, confirm bool) (*dynamodb.UpdateItemOutput, error) {
	table := os.Getenv("DB_TABLE_NAME")
	svc := dynamodb.New(session.New())

	input := &dynamodb.UpdateItemInput{
		// Provide the key to use for finding the right item.
		// Only matching on email means that a duplicate subscription request will override the first id.
		Key: map[string]*dynamodb.AttributeValue{
			"email": {
				S: aws.String(email),
			},
		},
		// Give the keys to be updated a shorthand to reference
		ExpressionAttributeNames: map[string]*string{
			"#ID": aws.String("id"),
			"#T": aws.String("timestamp"),
			"#C": aws.String("confirm"),
		},
		// Give the incoming values a shorthand to reference
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":idval": {
				S: aws.String(id),
			},
			":timeval": {
				S: aws.String(timestamp),
			},
			// Always override existing bool
			":confirmval": {
				BOOL: aws.Bool(confirm),
			},
		},
		// Use the shorthand refs to update these keys
		UpdateExpression: aws.String("SET #C = :confirmval, #T = :timeval, #ID = :idval"),
		TableName: aws.String(table),
	}

	result, err := svc.UpdateItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				log.Print(dynamodb.ErrCodeConditionalCheckFailedException, aerr.Error())
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Print(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Print(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Print(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeTransactionConflictException:
				log.Print(dynamodb.ErrCodeTransactionConflictException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				log.Print(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Print(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Print(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Print(err.Error())
		}
	}
	return result, err
}
