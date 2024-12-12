package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4j_tracing "github.com/raito-io/neo4j-tracing"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("neo4j_example")

func main() {
	shutdown, err := InstallExportPipeline()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Fatal(err.Error())
		}
	}()

	// Создаём общий контекст с таймаутом на 10 секунд
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx, span := tracer.Start(ctx, "main")
	defer span.End()

	// Параметры подключения
	uri := "neo4j://localhost:7687"
	username := "neo4j"
	password := "password"

	driverFactory := neo4j_tracing.NewNeo4jTracer()
	driver, err := driverFactory.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		panic(err)
	}

	// Создаем драйвер
	// driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	// if err != nil {
	// 	log.Fatalf("Ошибка подключения к Neo4j: %v", err)
	// }
	defer func() {
		_ = driver.Close(ctx)
	}()

	// Создаем сессию
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		_ = session.Close(ctx)
	}()

	// Запись данных
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Создание пользователей
		queries := []struct {
			query  string
			params map[string]interface{}
		}{
			{query: "MERGE (a:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Alice", "age": 30}},
			{query: "MERGE (b:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Bob", "age": 25}},
			{query: "MERGE (c:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Charlie", "age": 35}},
			{query: "MERGE (d:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Diana", "age": 28}},
			{query: "MERGE (e:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Eve", "age": 22}},
			{query: "MERGE (f:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Frank", "age": 40}},
			{query: "MERGE (g:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Grace", "age": 33}},
			{query: "MERGE (h:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Hank", "age": 29}},
			{query: "MERGE (i:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Ivy", "age": 31}},
			{query: "MERGE (j:Person {name: $name, age: $age})", params: map[string]interface{}{"name": "Jack", "age": 26}},
		}

		for _, q := range queries {
			_, err := tx.Run(ctx, q.query, q.params)
			if err != nil {
				return nil, err
			}
		}

		// Создание связей
		relationships := []struct {
			query  string
			params map[string]interface{}
		}{
			// Основные связи
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Alice", "name2": "Bob"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Bob", "name2": "Charlie"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Charlie", "name2": "Diana"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Diana", "name2": "Eve"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Eve", "name2": "Frank"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Frank", "name2": "Grace"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Grace", "name2": "Hank"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Hank", "name2": "Ivy"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Ivy", "name2": "Jack"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Jack", "name2": "Alice"}},

			// Дополнительные связи для усложнения графа
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Alice", "name2": "Charlie"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Bob", "name2": "Eve"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Charlie", "name2": "Frank"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Diana", "name2": "Hank"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Eve", "name2": "Ivy"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Frank", "name2": "Jack"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Grace", "name2": "Alice"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Hank", "name2": "Bob"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Ivy", "name2": "Charlie"}},
			{query: "MATCH (a:Person {name: $name1}), (b:Person {name: $name2}) MERGE (a)-[:FRIENDS]->(b)", params: map[string]interface{}{"name1": "Jack", "name2": "Diana"}},
		}

		for _, r := range relationships {
			_, err := tx.Run(ctx, r.query, r.params)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	if err != nil {
		log.Fatalf("Ошибка выполнения записи: %v", err)
	}

	// Чтение данных: находим друзей для Alice
	_, err = session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx,
			"MATCH (a:Person {name: $name})-[:FRIENDS]->(friend) "+
				"RETURN friend.name AS name, friend.age AS age",
			map[string]interface{}{"name": "Alice"})
		if err != nil {
			return nil, err
		}

		// Обработка результата
		fmt.Println("Друзья Alice:")
		for result.Next(ctx) {
			record := result.Record()
			name, _ := record.Get("name")
			age, _ := record.Get("age")
			fmt.Printf("- %s (Возраст: %d)\n", name, age)
		}

		return nil, result.Err()
	})
	if err != nil {
		log.Fatalf("Ошибка выполнения чтения: %v", err)
	}

	// Чтение данных: находим кратчайший путь от Alice до Diana
	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH p = shortestPath((a:Person {name: $start})-[:FRIENDS*]-(b:Person {name: $end}))
			RETURN p
		`
		params := map[string]interface{}{
			"start": "Alice",
			"end":   "Diana",
		}
		res, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		if res.Next(ctx) {
			path, _ := res.Record().Get("p")
			return path, nil
		}
		return nil, res.Err()
	})
	if err != nil {
		log.Fatalf("Ошибка выполнения запроса: %v", err)
	}

	// Вывод результата:
	if path, ok := result.(neo4j.Path); ok {
		fmt.Println("Кратчайший путь от Alice до Diana:")
		for _, node := range path.Nodes {
			if name, exists := node.Props["name"].(string); exists {
				fmt.Printf(" - %s\n", name)
			}
		}
	} else {
		fmt.Println("Путь не найден.")
	}

	// Чтение данных: находим самый длинный путь от Alice до Diana
	result, err = session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH p = (a:Person {name: $start})-[:FRIENDS*]-(b:Person {name: $end})
			RETURN p, length(p) AS pathLength
			ORDER BY pathLength DESC
			LIMIT 1
		`
		params := map[string]interface{}{
			"start": "Alice",
			"end":   "Diana",
		}
		res, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		if res.Next(ctx) {
			record := res.Record()
			path, _ := record.Get("p")
			return path, nil
		}
		return nil, res.Err()
	})
	if err != nil {
		log.Fatalf("Ошибка выполнения запроса: %v", err)
	}

	// Вывод результата:
	if path, ok := result.(neo4j.Path); ok {
		fmt.Println("Самый длинный путь от Alice до Diana:")
		for _, node := range path.Nodes {
			if name, exists := node.Props["name"].(string); exists {
				fmt.Printf(" - %s\n", name)
			}
		}
	} else {
		fmt.Println("Путь не найден.")
	}

	fmt.Println("Работа завершена успешно!")
}
