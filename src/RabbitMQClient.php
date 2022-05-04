<?php

/*
 * Author Thomas Beauchataud
 * Since 04/05/2022
 */

namespace TBCD\RabbitMQClient;

use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQClient
{

    /**
     * @var string
     */
    private string $host;

    /**
     * @var int
     */
    private int $port;

    /**
     * @var string
     */
    private string $user;

    /**
     * @var string
     */
    private string $password;

    /**
     * @var AMQPStreamConnection|null
     */
    private ?AMQPStreamConnection $connection;

    /**
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     */
    public function __construct(string $host, int $port, string $user, string $password)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
    }


    /**
     * @param string $body
     * @param string $exchange
     * @param string $routingKey
     * @return void
     * @throws Exception
     */
    public function send(string $body, string $exchange, string $routingKey): void
    {
        $connection = $this->getConnection();
        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
        ];
        $channel = new AMQPChannel($connection);
        $message = new AMQPMessage($body, $properties);
        $channel->basic_publish($message, $exchange, $routingKey);
    }

    /**
     * @param string $queueName
     * @return void
     * @throws Exception
     */
    public function createQueue(string $queueName): void
    {
        $queueName = strtoupper($queueName);
        $connection = $this->getConnection();
        $channel = new AMQPChannel($connection);
        $channel->queue_declare($queueName, false, true, false, false);
    }

    /**
     * @param string $queueName
     * @param bool $assertEmpty
     * @return void
     * @throws Exception
     */
    public function deleteQueue(string $queueName, bool $assertEmpty = true): void
    {
        $queueName = strtoupper($queueName);
        $connection = $this->getConnection();
        $channel = new AMQPChannel($connection);
        $channel->queue_delete($queueName, false, $assertEmpty);
    }

    /**
     * @param string $exchangeName
     * @return void
     * @throws Exception
     */
    public function createExchange(string $exchangeName): void
    {
        $exchangeName = strtoupper($exchangeName);
        $connection = $this->getConnection();
        $channel = new AMQPChannel($connection);
        $channel->exchange_declare($exchangeName, 'direct', false, true, false);
    }

    /**
     * @param string $exchangeName
     * @return void
     * @throws Exception
     */
    public function deleteExchange(string $exchangeName): void
    {
        $exchangeName = strtoupper($exchangeName);
        $connection = $this->getConnection();
        $channel = new AMQPChannel($connection);
        $channel->exchange_delete($exchangeName);
    }

    /**
     * @param string $exchangeName
     * @param string $queueName
     * @param string $routingKey
     * @return void
     * @throws Exception
     */
    public function bindQueue(string $exchangeName, string $queueName, string $routingKey = '*'): void
    {
        $exchangeName = strtoupper($exchangeName);
        $queueName = strtoupper($queueName);
        $routingKey = strtolower($routingKey);
        $connection = $this->getConnection();
        $channel = new AMQPChannel($connection);
        $channel->queue_bind($queueName, $exchangeName, $routingKey);
    }

    /**
     * @return AMQPStreamConnection
     */
    public function getConnection(): AMQPStreamConnection
    {
        if (!$this->connection) {
            $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->password);
        }

        return $this->connection;
    }
}