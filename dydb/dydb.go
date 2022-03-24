package dydb

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Map map[string]interface{}

func DeleteTable(tableName string) {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	// Delete table
	deleteParams := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	_, err := svc.DeleteTable(deleteParams)
	if err != nil {
		log.Fatalf("Got error calling DeleteTable: %s", err)
	}

	// Wait until table is deleted.
	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	err = svc.WaitUntilTableNotExists(describeParams)
	if err != nil {
		log.Fatalf("Got error calling WaitUntilTableNotExists: %s", err)
	}

	log.Println(fmt.Sprintf("Successfully deleted all items from table %s.", tableName))
}

func PutItems[T any](tableName string, items *[]T) {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	// Add each item to the dynamodb table
	for _, item := range *items {
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			log.Fatalf("Got error marshalling map: %s", err)
		}

		// Create item in table Movies
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}

		_, err = svc.PutItem(input)
		if err != nil {
			log.Fatalf("Got error calling PutItem: %s", err)
		}
	}

	log.Println(fmt.Sprintf("Successfully added items to table %s:\n", tableName))
}
