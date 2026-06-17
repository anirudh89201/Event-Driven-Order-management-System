package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var (
	dynamoClient *dynamodb.Client
	sqsclient    *sqs.Client
)

type products struct {
	Id          int     `json:"id"`
	Title       string  `json:"title"`
	Price       float64 `json:"price"`
	Description string  `json:"description"`
	Category    string  `json:"category"`
	Image       string  `json:"thumbnail"`
}
type apiResponse struct {
	Products []products `json:"products"`
}

func init() {
	godotenv.Load()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Error loading aws config: %v", err)
	}
	dynamoClient = dynamodb.NewFromConfig(cfg)
	sqsclient = sqs.NewFromConfig(cfg)
}

func (p products) String() string {
	return fmt.Sprintf(
		"Product{Id=%d, Title=%q, Price=%.2f, Description=%q, Category=%q, Image=%q}",
		p.Id,
		p.Title,
		p.Price,
		p.Description,
		p.Category,
		p.Image,
	)
}

func GetHttpResponse(url string) ([]products, error) {
	// 	client := &http.Client{}
	// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// 	defer cancel()

	// 	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("error creating request: %w", err)
	// 	}
	// 	resp, err := client.Do(req)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("error calling fakestore api: %w", err)
	// 	}
	// 	defer resp.Body.Close()

	// 	var res apiResponse
	// 	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
	// 		return nil, fmt.Errorf("error decoding response: %w", err)
	// 	}
	// 	return res.Products, nil
	return []products{
		// electronics → location 10
		{Id: 1, Title: "USB Charger", Price: 150, Category: "electronics", Description: "Fast USB charger", Image: "electronics.jpg"},

		// clothing → location 20
		{Id: 2, Title: "Denim Jacket", Price: 999, Category: "clothing", Description: "Classic denim jacket", Image: "jacket.jpg"},

		// books → location 30
		{Id: 3, Title: "Go Programming", Price: 499, Category: "books", Description: "Learn Go from scratch", Image: "book.jpg"},

		// home → location 40
		{Id: 4, Title: "Kitchen Blender", Price: 1299, Category: "home", Description: "High speed blender", Image: "blender.jpg"},

		// health/beauty → location 50
		{Id: 5, Title: "Face Moisturizer", Price: 349, Category: "beauty", Description: "Daily moisturizer", Image: "cream.jpg"},

		// fragile → location 60
		{Id: 6, Title: "Glass Vase", Price: 799, Category: "fragile", Description: "Handmade glass vase", Image: "vase.jpg"},

		// price > 2000 → locType W
		{Id: 7, Title: "Gaming Laptop", Price: 85000, Category: "electronics", Description: "High performance laptop", Image: "laptop.jpg"},

		// default → location 99
		{Id: 8, Title: "Mystery Box", Price: 199, Category: "random", Description: "Surprise product", Image: "box.jpg"},

		// price = 0 → triggers ERROR path ❌
		{Id: 9, Title: "Free Sample", Price: 0, Category: "beauty", Description: "Free product sample", Image: "sample.jpg"},

		// duplicate of id 1 → tests idempotency ❌
		{Id: 1, Title: "USB Charger", Price: 150, Category: "electronics", Description: "Fast USB charger", Image: "electronics.jpg"},
	}, nil
}

func calculateHash(p products) string {
	raw := fmt.Sprintf("%d|%s|%s", p.Id, p.Title, p.Description)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func isDuplicate(ctx context.Context, hash string) (bool, error) {
	result, err := dynamoClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(os.Getenv("DYNAMODB_TABLENAME")),
		Key: map[string]types.AttributeValue{
			"hash": &types.AttributeValueMemberS{Value: hash},
		},
	})
	if err != nil {
		return false, fmt.Errorf("error checking dynamodb: %w", err)
	}
	return result.Item != nil, nil
}

func saveProcessing(ctx context.Context, hashvalue string, product products) error {
	_, err := dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(os.Getenv("DYNAMODB_TABLENAME")),
		Item: map[string]types.AttributeValue{
			"hash":        &types.AttributeValueMemberS{Value: hashvalue},
			"id":          &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", product.Id)},
			"title":       &types.AttributeValueMemberS{Value: product.Title},
			"description": &types.AttributeValueMemberS{Value: product.Description},
			"Category":    &types.AttributeValueMemberS{Value: product.Category},
			"status":      &types.AttributeValueMemberS{Value: "PROCESSING"},
			"Price": &types.AttributeValueMemberN{Value: fmt.Sprintf("%.2f", product.Price)},
		},
	})
	return err
}
func SendtoSQS(ctx context.Context, product products) error {
	body, err := json.Marshal(product)
	if err != nil {
		return fmt.Errorf("Error formatting product:%w", err)
	}
	_, err = sqsclient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(os.Getenv("SQS_URL")),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("Error pushing to SQS:%w", err)
	}
	return nil
}
func handler(ctx context.Context) error {
	const url = "https://fakestoreapi.com/products"

	products, err := GetHttpResponse(url)
	if err != nil {
		return fmt.Errorf("error fetching products: %w", err) // ← return error properly
	}

	processed := 0
	for _, product := range products {
		hashvalue := calculateHash(product)
		duplicate, err := isDuplicate(ctx, hashvalue)
		if err != nil {
			return err
		}

		if duplicate {
			log.Printf("Duplicate found, ignoring product_id: %d hash value of it is :%s", product.Id, hashvalue)
			continue
		}

		if err := saveProcessing(ctx, hashvalue, product); err != nil {
			return err
		}
		log.Printf("Saved product_id: %d to DynamoDB", product.Id)
		if err := SendtoSQS(ctx, product); err != nil {
			return err
		}
		processed += 1
		log.Printf("Sent to SQS %d", processed)

	}
	return nil
}

func main() {
	// ctx := context.Context(context.TODO())
	lambda.Start(handler)
	// if err := handler(ctx); err != nil {
	// 	log.Printf("The error is :%v", err)
	// }
}
