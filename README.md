# Phusion: An Integration Framework

## What is Phusion?

**Phusion** (pronounced as "fusion"), full name Phusion Integration, **is an open source programming framework and runtime engine for building enterprise integration applications**.

For example, with Phusion, you can integrate your company website with your CRM system, EMail system, Reporting system, and so on. When a visitor leaves a product inquiry message on your website, Phusion automatically sends her/him a greeting email in response, and creates a new lead in your CRM system, and informs the regional sales of this new business opportunity.

Existing open source integration frameworks, such as Apache Camel and Spring Integration, are excellent and widely used. But compared with them, Phusion focuses on enhancing the following capabilities:

- **Decouple the work of encapsulating APIs from composing integration logic**. APIs should be encapsulated by developers who are familiar with the protocol, terminology and functionality of the APIs. And integration logic should be composed by developers who are in direct contact with the customers, familiar with their jargons and understand what they want. These two groups of developers may have different skill sets and even belong to different organizations. Therefore, their code should be separated, and their artifacts should also be deployed independently. This is exactly what Phusion does and what other frameworks pay less attention to.

- **Standardized integration schemes**. Based on real-world scenarios, define and utilize standard messages to simplify integration. Take the previous example of collecting visitor messages into CRM systems, if the message format is standardized, and the corresponding APIs are encapsulated according to the standard, then the same integration application can work with different web form tools and CRM systems. This reduces the integration complexity from o(n^2) to o(n).

- **Provide common facilities for application development**, such as web port, scheduler, database, memory storage, file system, clustering, etc., so that the developers do not need to take care of these environment settings.

- **Provide runtime engine to execute the integration applications and Web console to manage them**. For example, dynamically load and start applications, configure integration workflows in real time, query and analyze transaction data, monitor cluster status, and more.

As for iPaaS services like IFTTT, Zapier, Microsoft Flow, they are great for non-technical users and office automation scenarios, but not for serious software development. Sometimes, they are not flexible enough to allow programmers to build their own enterprise-grade integration applications.

<br/>

## Application

In Phusion, the term **application** has a specific meaning. It is a component that wraps an external system's API as one or more **endpoints** for Phusion to integrate. Each endpoint carries a specific function. From the perspective of the caller, an endpoint can be:
- **Inbound**: the caller is calling from outside to the Phusion application. The application acts as the server, providing the calling address. For an inbound endpoint, it emits a **output** message to the Phusion engine, and receives an **input** message as the response.
- **Outbound**: the caller (mostly a step in an integration workflow) is inside the Phusion system, and the application delegates the caller to access an outside service. For an outbound endpoint, it receives an **input** message from the Phusion engine, and emits an **output** message as the response.

An application can be used by multiple clients, each client has its own credentials and settings to access the application. The collection of credentials and settings belonging to the client is called the client's **connection** to the application. Applications are responsible for maintaining the connections on behalf of the clients, in order to fluently use the application endpoints in integrations.

The lifecycle of an application:
- None: the application is not registered to the engine, that is, it is unknown to the engine. This status is also referred to as "Unused".
- Stopped: the application is registered, but is not executing.
- Running: the application is registered, and is executing periodically or ready to service the incoming messages.

<br/>

## Integration

An **integration** connects application endpoints with workflow steps to perform certain functions on behalf of the client. The steps can be:
- Endpoint: call an outbound application endpoint.
- Java: execute a Java processor.
- JavaScript: execute a JavaScript processor.
- Direct: emit some message to the following step.
- ForEach: iterate over a list of items (JSON Array), for each item, execute a block of sub-workflow.
- Collect: collect the results of the previous sub-workflow steps into a single message (JSON Array).

An integration can be trigger to execution by:
- Endpoint: an inbound application endpoint is called by outside systems.
- Timer: schedule to execute the integration.

When an integration is executed, a data structure **transaction** is created to convey the message, properties, and context of the instance of the integration. The transaction is passed to each step, and each step can modify the transaction data (message and properties).

An integration has similar lifecycle as applications:
- None: the integration is not registered to the engine, that is, it is unknown to the engine. This status is also referred to as "Unused".
- Stopped: the integration is registered, but is not executing.
- Running: the integration is registered, and is executing periodically or ready to service.

<br/>

## Client

A **client** is an organization who wants to implement and execute integration applications. A client usually owns some connections to certain applications, and some integrations.

<br/>

## Engine


<!--

https://www.markdownguide.org/basic-syntax

## Engine, Engine API, Cluster, Express Engine and Service.
应用生命周期管理
流程生命周期管理
应用工具包
任务调度
网络通信接口
同步转异步
重试
限流
状态存储
日志与监控
远程管理 API

隶属于同一集群的聚变平台提供统一的分布式任务调度服务，可以实现定时、异步任务执行。

同一集群中，相同 ID 的从固定时刻开始的任务将只在集群中某一个聚变节点上执行。如果是从任意时刻开始的周期性任务，由于无法判断这些任务之间在时间上的互斥关系，将不能保证只在一个聚变节点上执行。

隶属于同一集群的聚变平台提供统一的分布式内存键值（Key-Value）数据存储服务。

聚变平台上运行的应用程序都能接收到 Context ctx 参数，从中可以获取到聚变平台的运行引擎（Engine）。通过引擎可以获取到键值存储服务的 KVStorage 对象。

隶属于同一集群的聚变平台提供统一的关系型数据存储服务。为了保证聚变平台的整体性能，仅提供必要的数据操作能力。除了新增、修改、删除等数据操作外，仅支持行（记录）级的简单查询操作，不支持聚合统计等复杂查询。

聚变平台上运行的应用程序都能接收到 Context ctx 参数，从中可以获取到聚变平台的运行引擎（Engine）。通过引擎可以获取到数据存储服务的 DBStorage 对象。

隶属于同一集群的聚变平台提供统一的分布式文件存储服务。支持两类文件：
● 私有文件：不可被外网访问到。
● 公开文件（Public file）：可以通过网址访问到。


不同的引擎，相同的组件 Embeddable
开发引擎：用于开发组件的 Mock 环境
单体引擎：一个引擎是一个进程，加载并运行各个组件，启动多进程进行容错
集群引擎：各组件有独立的进程，独立开发和运行，整体采用事件驱动的无服务器架构；各引擎组成集群，各应用、流程可运行在独立的主机进程中


### Phusion Express Engine
Express is a lightweight version of Phusion Engine that implements the Phusion API. It is compact, but clustered and battle-tested in real world use cases. The Express Engine also comes with a web service to use and manage the engine.

Express 架构
Engine 引擎、Application 应用、Integration 集成流程、Connection 连接、Client 客户、Transaction 实例；Protocol 场景标准、Template 模板
Express：Engine + Service；P2P、Cloud (serverless)、App Hub、AI Coding
集群（Redis Queue）、定时（Quartz）、V8引擎（ThreadLocal）、数据采集与统计（Context）、内嵌Tomcat、路由、动态加载Jar/JS/Node、BaseApp

Service console UI and API: 集群、应用（数据表）、集成流程（流程设置及配置）、实例（搜索）

To Do
Service Web console UI
Clustering
组件集市，类似于 Maven 的运作方式
Embedded 模式的引擎、SDK 快速开发 API 或者使用别人的 API
Express, Cloud, P2P/Federated/decenter
Embeddable (premise deployment)
Cloud engine: for semi-trustful application/integration, huge traffic.
PaaS engine: for not trustful application/integration, multi-tenant, huge traffic.
P2P engine: self-trust, connected with ready application (no need to install application).
Express engine: private and trust.
Dev engine: sandbox for develop application and processor.
Other to does: Data schema and remove serialization for each step; New JS engine; Polyglot support.
支持http之外的协议、多语言支持、SDK无API自主接入

Application Use cases:
XCharge

Programming
Engine API (Java, JavaScript), Develope Application (HttpBaseApplication), Integration
https://www.yuque.com/yiting-eh5ph/thfyr2/kssuo7 Java、JavaScript
聚变应用必须实现 Application 接口
本文档只介绍如何开发和测试 HTTP 应用，即应用系统中的所有服务接口均采用 HTTP/1.1 协议。

Management
API Doc
https://www.apifox.cn/apidoc/shared-384b25d2-03e3-4e8a-a664-5d66db922a89

Get Started
Publish jars, Install, Run, Sample Application and integration (load and run)

-->