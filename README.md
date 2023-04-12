# Phusion Integration

## What is Phusion

<br/>

Phusion Integration, abbreviated as **Phusion** (pronounced as "fusion"), **is an open source programming framework and runtime engine for building enterprise integration applications**.

For example, with Phusion, you can integrate your company website with your CRM system, EMail system, Reporting system, and so on. When a visitor leaves a product inquiry message on your website, Phusion automatically sends her/him a greeting email in response, and creates a new lead in your CRM system, and informs the regional sales of this new business opportunity.

<br/>

Existing open source integration frameworks, such as Apache Camel and Spring Integration, are excellent and widely used. But compared with them, Phusion focuses on enhancing the following capabilities:

**1. Decouple the work of encapsulating APIs from composing integration workflow**. APIs should be encapsulated by developers who are familiar with the protocol, terminology and functionality of the APIs. And integration workflows should be composed by developers who are in direct contact with the customers, familiar with their jargons and understand what they want. These two groups of developers may have different skill sets and even belong to different organizations. Therefore, their code should be separated, and their artifacts should also be deployed independently. This is exactly what Phusion does and what other frameworks pay less attention to.

**2. Standardized integration schemes**. Based on real-world scenarios, define and utilize standard messages to simplify integration. Take the previous example of collecting visitor messages into CRM systems, if the message format is standardized, and the corresponding APIs are encapsulated according to the standard, then the same integration application can work with different web form tools and CRM systems. This reduces the integration complexity from o(n^2) to o(n).

**3. Provide common facilities for application development**, such as web port, scheduler, database, memory storage, file system, clustering, etc., so that the developers do not need to take care of these environment settings.

**4. Provide runtime engine to execute the integration applications and Web console to manage them**. For example, dynamically load and start applications, configure integration workflows in real time, query and analyze transaction data, monitor cluster status, and more.

<br/>

As for iPaaS services like IFTTT, Zapier, Microsoft Flow, they are great for non-technical users and office automation scenarios, but not for serious software development. Mostly, they are not flexible enough to allow programmers to build their own enterprise-grade integration applications.

<br/>

<!--

## Concepts and Modules



----------

https://www.markdownguide.org/basic-syntax

Express is a lightweight version of Phusion Engine that implements the Phusion API. It is compact, but clustered and battle-tested in real world use cases. The Express Engine also comes with a web service to use and manage the engine.

目前的 iPaaS，要么是面向非技术人员的零代码，要么是面向技术人员的一整套框架。非技术人员搞不定，技术人员用起来效率也很低。
我们主要是面向技术人员，但会最大限度降低技术人员的学习成本。

不同的引擎，相同的组件
开发引擎：用于开发组件的 Mock 环境
单体引擎：一个引擎是一个进程，加载并运行各个组件，启动多进程进行容错
集群引擎：各组件有独立的进程，独立开发和运行，整体采用事件驱动的无服务器架构；各引擎组成集群，各应用、流程可运行在独立的主机进程中

集成流程可以由用户使用脚本直接在系统界面中开发和测试
服务接口封装代码由社区提供、由聚变构建部署。安全性？可靠性？
引擎由聚变平台提供和维护

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

为应用系统（Application）开发服务接口封装程序，以便在聚变平台中使用这些接口。

关于聚变应用
聚变应用（Application）将一个应用系统对外提供的、将要用于系统集成的服务接口（Endpoint）封装成指定规格的组件，以方便用户在集成流程（Integration）中使用这些接口。聚变应用必须实现 Application 接口。

应用的生命周期：
● 创建（Init）：加载并初始化应用，然后将其置于 stopped 状态；
● 启动（Start）：启动应用使其能执行服务任务，此时处于 running 状态；
● 停止（Stop）：暂停应用，不再执行服务任务，此时回到 stopped 状态；
● 销毁（Destroy）：停止应用，并释放其资源，将其从聚变平台销毁。

应用中服务接口的调用方向可以是：
● 呼入接口（Inbound endpoint）：由聚变平台（即聚变平台上运行的应用）提供服务地址，由外部应用系统调用该地址向聚变平台发送信息。
● 呼出接口（Outbound endpoint）：由聚变平台（即聚变平台上运行的应用）调用外部应用系统的服务，向其发送信息。

聚变平台上的同一个应用可能被多个客户使用，每个客户都以自己的身份来访问这些接口，而这些身份认证、授权策略是由外部应用系统的服务接口规定的。聚变应用须为每个客户维持其身份信息，并根据接口规则来执行相应的认证、授权操作，此称为连接（Connection）。

本文档只介绍如何开发和测试 HTTP 应用，即应用系统中的所有服务接口均采用 HTTP/1.1 协议。

隶属于同一集群的聚变平台提供统一的分布式任务调度服务，可以实现定时、异步任务执行。

同一集群中，相同 ID 的从固定时刻开始的任务将只在集群中某一个聚变节点上执行。如果是从任意时刻开始的周期性任务，由于无法判断这些任务之间在时间上的互斥关系，将不能保证只在一个聚变节点上执行。

隶属于同一集群的聚变平台提供统一的分布式内存键值（Key-Value）数据存储服务。

聚变平台上运行的应用程序都能接收到 Context ctx 参数，从中可以获取到聚变平台的运行引擎（Engine）。通过引擎可以获取到键值存储服务的 KVStorage 对象。

隶属于同一集群的聚变平台提供统一的关系型数据存储服务。为了保证聚变平台的整体性能，仅提供必要的数据操作能力。除了新增、修改、删除等数据操作外，仅支持行（记录）级的简单查询操作，不支持聚合统计等复杂查询。

聚变平台上运行的应用程序都能接收到 Context ctx 参数，从中可以获取到聚变平台的运行引擎（Engine）。通过引擎可以获取到数据存储服务的 DBStorage 对象。

隶属于同一集群的聚变平台提供统一的分布式文件存储服务。支持两类文件：
● 私有文件：不可被外网访问到。
● 公开文件（Public file）：可以通过网址访问到。

聚变平台上运行的应用程序都能接收到 Context ctx 参数，从中可以获取到聚变平台的运行引擎（Engine）。通过引擎可以获取到文件存储服务的 FileStorage 对象。

架构
Engine 引擎、Application 应用、Integration 集成流程、Connection 连接、Client 客户、Transaction 实例；Protocol 场景标准、Template 模板
Express：Engine + Service；P2P、Cloud (serverless)、App Hub、AI Coding
集群（Redis Queue）、定时（Quartz）、V8引擎（ThreadLocal）、数据采集与统计（Context）、内嵌Tomcat、路由、动态加载Jar/JS/Node、BaseApp

To Do
Web UI
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

管理
集群、应用（数据表）、集成流程（流程设置及配置）、实例（搜索）

开发
https://www.yuque.com/yiting-eh5ph/thfyr2/kssuo7 Java、JavaScript
https://www.apifox.cn/apidoc/shared-384b25d2-03e3-4e8a-a664-5d66db922a89
引擎开发模式、Demo、Java 部署；Java、JavaScript 代码；上传文件
https://github.com/OpenPhusion/xcharge Issue, Clone, Pull Req First

-->