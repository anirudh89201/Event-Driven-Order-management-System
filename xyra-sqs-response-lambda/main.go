package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

type Product struct {
	Id          int    `json:"id"`
	Title       string `json:"title"`
	Price       int    `json:"price"`
	Description string `json:"description"`
	Category    string `json:"category"`
}

type Result struct {
	Product         Product `json:"product"`
	CurrentLocation int     `json:"location"`
	LocType         string  `json:"loc_type"`
}

var (
	dynamoclient *dynamodb.Client
	s3Client     *s3.Client
)

func init() {
	godotenv.Load()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Configuration error")
	}
	dynamoclient = dynamodb.NewFromConfig(cfg)
	s3Client = s3.NewFromConfig(cfg)
	// validate env vars on startup
	required := []string{
		"DYNAMODB_TABLENAME",
		"ERROR_BUCKET_NAME",
		"SUCCESS_BUCKET_NAME",
	}
	for _, env := range required {
		if os.Getenv(env) == "" {
			log.Fatalf("Missing required environment variable: %s", env)
		}
	}
}
func DispatchAlgorithm(product *Product) (Result, error) {
	category := strings.ToLower(strings.TrimSpace(product.Category))
	location := 99
	locType := "S"

	switch {
	case strings.Contains(category, "electronic") || strings.Contains(category, "appliance") || strings.Contains(category, "computer"):
		location = 10
		locType = "S" // electronics
	case strings.Contains(category, "clothing") || strings.Contains(category, "apparel") || strings.Contains(category, "fashion"):
		location = 20
		locType = "W" // clothing
	case strings.Contains(category, "book") || strings.Contains(category, "stationery") || strings.Contains(category, "media"):
		location = 30
		locType = "W" // books / media
	case strings.Contains(category, "home") || strings.Contains(category, "garden") || strings.Contains(category, "kitchen"):
		location = 40
		locType = "S" // home goods
	case strings.Contains(category, "health") || strings.Contains(category, "beauty") || strings.Contains(category, "personal"):
		location = 50
		locType = "W" // personal care
	case strings.Contains(category, "fragile") || strings.Contains(category, "glass") || strings.Contains(category, "ceramic"):
		location = 60
		locType = "S" // fragile goods
	default:
		location = 99
		locType = "W" // standard/unmatched
	}

	// High-price items get priority dispatch
	if product.Price > 2000 {
		locType = "W"
	}
	if product.Price == 0 {
		return Result{Product: *product, CurrentLocation: -666, LocType: "S"}, errors.New("Product Price is equal to zero payment not captured properly..")
	}
	return Result{
		Product:         *product,
		CurrentLocation: location,
		LocType:         locType,
	}, nil
}

func (p *Product) ToString() string {
	return fmt.Sprintf("ID: %d title: %s price : %d description: %s category: %s", p.Id, p.Title, p.Price, p.Description, p.Category)
}
func MarkOrderInDynamoDB(ctx context.Context, result *Result, errorCh chan error, StatusValue string) {
	QueryId := fmt.Sprintf("%d", result.Product.Id)
	res, err := dynamoclient.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(os.Getenv("DYNAMODB_TABLENAME")),
		IndexName:              aws.String("Id_index"),
		KeyConditionExpression: aws.String("id = :id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberN{Value: QueryId},
		},
	})
	if err != nil {
		errorCh <- fmt.Errorf("unable to query order by GSI Id_index: %w", err)
		close(errorCh)
		return
	}
	if len(res.Items) == 0 {
		errorCh <- fmt.Errorf("order not found by GSI Id_index for id %s", QueryId)
		close(errorCh)
		return
	}
	hash := res.Items[0]["hash"].(*types.AttributeValueMemberS).Value
	_, err = dynamoclient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(os.Getenv("DYNAMODB_TABLENAME")),
		Key: map[string]types.AttributeValue{
			"hash": &types.AttributeValueMemberS{Value: hash},
		},
		UpdateExpression: aws.String("SET #s = :status"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: StatusValue},
		},
	})
	if err != nil {
		errorCh <- fmt.Errorf("unable to update the order to %s: %w", StatusValue, err)
		close(errorCh)
		return
	}
	close(errorCh)
}
func InsertIntoS3(ctx context.Context, result *Result, s3InsertError chan error, s3BucketName string) {
	data, err := json.Marshal(result)
	if err != nil {
		s3InsertError <- fmt.Errorf("Error converting the data into JSON format: %w", err)
		close(s3InsertError)
		return
	}
	key := fmt.Sprintf("%s/product-%d.json",
		time.Now().Format("2006-01-02"),
		result.Product.Id,
	)
	_, err1 := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s3BucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err1 != nil {
		s3InsertError <- fmt.Errorf("error uploading errored order to S3: %w", err1)
		close(s3InsertError)
		return
	}
	close(s3InsertError)
}
func ProcessOrder(ctx context.Context, result Result, S3BucketName string, DynamoDBStatus bool) error {
	var val strings.Builder
	if !DynamoDBStatus {
		val.WriteString("Errored")
	} else {
		val.WriteString("Success")
	}
	dynamoChannel := make(chan error, 1)
	S3Channel := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		MarkOrderInDynamoDB(ctx, &result, dynamoChannel, val.String())
	}()
	go func() {
		defer wg.Done()
		InsertIntoS3(ctx, &result, S3Channel, S3BucketName)
	}()
	wg.Wait()
	dyanmoError := <-dynamoChannel
	s3Error := <-S3Channel
	if dyanmoError != nil {
		log.Printf("Dyanmo error:%v", dyanmoError)
		return dyanmoError
	}
	if s3Error != nil {
		log.Printf("S3 Error:%v", s3Error)
		return s3Error
	}
	return nil

}
func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, record := range sqsEvent.Records {
		fmt.Printf("Message ID : %s\n", record.MessageId)
		fmt.Printf("Raw Body : %s\n", record.Body)
		var product Product
		if err := json.Unmarshal([]byte(record.Body), &product); err != nil {
			return fmt.Errorf("Error while unmarshalling the json response we got: %w", err)
		}

		result, err := DispatchAlgorithm(&product)
		if err != nil {
			BucketName := os.Getenv("ERROR_BUCKET_NAME")
			if err := ProcessOrder(ctx, result, BucketName, false); err != nil {
				return fmt.Errorf("Error Processing Order:%v", err)
			}
		} else {
			BucketName := os.Getenv("SUCCESS_BUCKET_NAME")
			if err := ProcessOrder(ctx, result, BucketName, true); err != nil {
				return fmt.Errorf("Error Processing Order:%v", err)
			}
		}
		fmt.Printf("Dispatch result: location=%d loc_type=%s product=%s\n", result.CurrentLocation, result.LocType, product.ToString())
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
