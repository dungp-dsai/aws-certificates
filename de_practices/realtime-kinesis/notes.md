1. Do we need to setting the the VPC? and subnets? 
No we don't need that. We can use the default network setup in AWS. Setting up VPC and subnets only when we need to protect better and comply with compliance and security.

2. Why we need to creat the IAM role?

3. Why we need to ensure network setting in the MSK cluster enable access from EC2


4. we need to create the MSK cluster without the serverless option because it wont be able to creat topics under the serverless option? why?

5. We open public access from the MSK cluster is definitely not a good practice. We just open it to test how the API gateway can access the private cluster Kafka

6. What is layers in Lambda function? why we need it?
To solve the missing packages and env issues?