package main

import (
	"context"
	"log"
	"time"

	pb "github.com/Shulammite-Aso/mailinglist/proto"
	"github.com/alexflint/go-arg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Util function to log responses from the gRPC server

func logResponse(res *pb.EmailResponse, err error) {
	if err != nil {
		log.Fatalf("  error: %v", err)
	}

	if res.EmailEntry == nil {
		log.Printf("  email not found")
	} else {
		log.Printf("  response: %v", res.EmailEntry)
	}
}

func createEmail(client pb.MailingListServiceClient, addr string) *pb.EmailEntry {
	log.Println("create email")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.CreateEmail(ctx, &pb.CreateEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmail(client pb.MailingListServiceClient, addr string) *pb.EmailEntry {
	log.Println("get email")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmail(ctx, &pb.GetEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

func getEmailBatch(client pb.MailingListServiceClient, count int, page int) {
	log.Println("get email batch")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetEmailBatch(ctx, &pb.GetEmailBatchRequest{Count: int64(count), Page: int64(page)})
	if err != nil {
		log.Fatalf("  error: %v", err)
	}
	log.Println("response:")
	for i := 0; i < len(res.EmailEntries); i++ {
		log.Printf("  item [%v of %v]: %s", i+1, len(res.EmailEntries), res.EmailEntries[i])
	}
}

func updateEmail(client pb.MailingListServiceClient, entry pb.EmailEntry) *pb.EmailEntry {
	log.Println("update email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.UpdateEmail(ctx, &pb.UpdateEmailRequest{EmailEntry: &entry})
	logResponse(res, err)

	return res.EmailEntry
}

func deleteEmail(client pb.MailingListServiceClient, addr string) *pb.EmailEntry {
	log.Println("delete email")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.DeleteEmail(ctx, &pb.DeleteEmailRequest{EmailAddr: addr})
	logResponse(res, err)

	return res.EmailEntry
}

var args struct {
	GrpcAddr string `arg:"env:MAILINGLIST_GRPC_ADDR"`
}

func main() {
	arg.MustParse(&args)

	if args.GrpcAddr == "" {
		args.GrpcAddr = ":8081"
	}

	connect, err := grpc.Dial(args.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer connect.Close()

	client := pb.NewMailingListServiceClient(connect)

	newEmail := createEmail(client, "hellofour@hello.com")
	newEmail.ConfirmedAt = 1000
	updateEmail(client, *newEmail)
	deleteEmail(client, newEmail.Email)
	getEmailBatch(client, 5, 1)
}
