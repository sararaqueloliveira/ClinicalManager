# ClinicalManagment_PSR
Projeto da cadeira de "Projeto de Sistemas em Redes" que consiste numa aplicação Web sobre a gestão de serviços clínicos, implementado em Golang (FHIR server) e Angular 7.

----
Inicialização do Servidor Mongodb:

> mongod --dbpath "C:\MongoDB" --replSet rs0

> rs.initiate()

Caso o mongo não esteja com utilizador primário correr o seguinte comando:

> db.adminCommand( { shutdown : 1} )

----

Inicialização do Servidor FHIR:

> go run server.go
  -> Na pasta do server.go (pasta middleware)
  
Caso este comando não funcione, correr este ficheiro no IDE Pycharm.

----

Inicialização do Cliente em Angular:

-> Ter o node.js instalado;
-> Instalar a cli de angular: > npm install -g @angular/cli
-> Correr o comando:

> ng serve
  -> Na pasta client
  
-----
 
Servidor: localhost:3001
Cliente: localhost:4200




