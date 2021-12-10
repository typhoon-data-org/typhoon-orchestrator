-- MySQL dump 10.13  Distrib 8.0.23, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: prod_web_ecom
-- ------------------------------------------------------
-- Server version	8.0.23-0ubuntu0.20.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


CREATE DATABASE prod_web_ecom;
USE prod_web_ecom; 


--
-- Table structure for table `clients`
--

DROP TABLE IF EXISTS `clients`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `clients` (
  `row_id` bigint DEFAULT NULL,
  `email_id` varchar(150) DEFAULT NULL,
  `prefix` varchar(150) DEFAULT NULL,
  `name` varchar(250) DEFAULT NULL,
  `birth_date` varchar(150) DEFAULT NULL,
  `phone_number` varchar(150) DEFAULT NULL,
  `additional_email_id` varchar(150) DEFAULT NULL,
  `address` varchar(255) DEFAULT NULL,
  `zip_code` varchar(45) DEFAULT NULL,
  `city` varchar(150) DEFAULT NULL,
  `state` varchar(150) DEFAULT NULL,
  `country` varchar(150) DEFAULT NULL,
  `year` varchar(150) DEFAULT NULL,
  `time` varchar(150) DEFAULT NULL,
  `link` varchar(250) DEFAULT NULL,
  `text` varchar(250) DEFAULT NULL,
  `creation_timestamp` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT=',Email Id,Prefix,Name,Birth Date,Phone Number,Additional Email Id,Address,Zip Code,City,State,Country,Year,Time,Link,Text';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `clients`
--

LOCK TABLES `clients` WRITE;
/*!40000 ALTER TABLE `clients` DISABLE KEYS */;
INSERT INTO `clients` VALUES (0,'Clayton.Miller@testDomain.com','Mr.','Nicole Clark','06-10-1998','+44113 4960146','kellyraven@stewart-miller.com','595 Pope Drive\nWest Victor, MA 26556','89580','Amberbury','Michigan','United Arab Emirates','1998','09:23:52','https://richardson-martin.info/','race','2021-05-20 13:20:33'),(1,'Thomas.Wilson@testDomain.com','Mr.','Dakota Castro','07-06-1983','(0118) 4960013','michaelhancock@gmail.com','810 Sergio Terrace\nGomezchester, SD 58101','66478','North Randy','Hawaii','American Samoa','2004','15:35:08','http://heath.com/','piece','2021-05-20 13:20:33'),(2,'Dr..Monica@testDomain.com','Misc.','Kyle Williams','25-06-1974','+44151 496 0164','cynthia71@gmail.com','26526 Benjamin Ways Apt. 480\nNorth Jessicaland, CA 48645','76492','Schmittmouth','Arkansas','Cyprus','2001','06:51:24','https://garrett.com/','campaign','2021-05-20 13:20:33'),(3,'Todd.Luna@testDomain.com','Mr.','Elizabeth Carroll','19-12-1998','01214960167','smithpenny@martin-shelton.com','87112 Isabel Ramp\nBethanyview, TN 69618','33265','Kimberlyhaven','Minnesota','Liechtenstein','1992','20:37:27','https://www.silva-joseph.com/','push','2021-05-20 13:20:33'),(4,'Jeremy.Thomas@testDomain.com','Dr.','Jesus Thompson','08-06-1992','(0118)4960069','kylie30@wilkins.com','476 Bonnie Circles\nNew Elizabeth, RI 61983','40802','Adrianfurt','New Jersey','Estonia','1974','09:18:44','http://mitchell.com/','bank','2021-05-20 13:20:33'),(5,'Reginald.Morris@testDomain.com','Mrs.','Joel Holland','02-11-1972','0141 496 0115','robbinsmelanie@gmail.com','34416 Ellis Extension Suite 421\nLake Stephanie, MA 71428','30086','North Timothystad','Alabama','Spain','2015','18:09:52','https://terry-clark.com/','matter','2021-05-20 13:20:33'),(6,'James.Brooks@testDomain.com','Ms.','Steven Kramer','06-12-1974','(0114) 4960841','darmstrong@hotmail.com','436 Kim Gardens Apt. 042\nSouth Mark, MI 32684','7154','North James','Minnesota','Burkina Faso','2014','00:20:22','http://www.padilla-castillo.org/','as','2021-05-20 13:20:33'),(0,'Clayton.Miller@testDomain.com','Mr.','Nicole Clark','06-10-1998','+44113 4960146','kellyraven@stewart-miller.com','595 Pope Drive\nWest Victor, MA 26556','89580','Amberbury','Michigan','United Arab Emirates','1998','09:23:52','https://richardson-martin.info/','race','2021-05-20 13:20:33'),(1,'Thomas.Wilson@testDomain.com','Mr.','Dakota Castro','07-06-1983','(0118) 4960013','michaelhancock@gmail.com','810 Sergio Terrace\nGomezchester, SD 58101','66478','North Randy','Hawaii','American Samoa','2004','15:35:08','http://heath.com/','piece','2021-05-20 13:20:33'),(2,'Dr..Monica@testDomain.com','Misc.','Kyle Williams','25-06-1974','+44151 496 0164','cynthia71@gmail.com','26526 Benjamin Ways Apt. 480\nNorth Jessicaland, CA 48645','76492','Schmittmouth','Arkansas','Cyprus','2001','06:51:24','https://garrett.com/','campaign','2021-05-20 13:20:33'),(3,'Todd.Luna@testDomain.com','Mr.','Elizabeth Carroll','19-12-1998','01214960167','smithpenny@martin-shelton.com','87112 Isabel Ramp\nBethanyview, TN 69618','33265','Kimberlyhaven','Minnesota','Liechtenstein','1992','20:37:27','https://www.silva-joseph.com/','push','2021-05-20 13:20:33'),(4,'Jeremy.Thomas@testDomain.com','Dr.','Jesus Thompson','08-06-1992','(0118)4960069','kylie30@wilkins.com','476 Bonnie Circles\nNew Elizabeth, RI 61983','40802','Adrianfurt','New Jersey','Estonia','1974','09:18:44','http://mitchell.com/','bank','2021-05-20 13:20:33'),(5,'Reginald.Morris@testDomain.com','Mrs.','Joel Holland','02-11-1972','0141 496 0115','robbinsmelanie@gmail.com','34416 Ellis Extension Suite 421\nLake Stephanie, MA 71428','30086','North Timothystad','Alabama','Spain','2015','18:09:52','https://terry-clark.com/','matter','2021-05-20 13:20:33'),(6,'James.Brooks@testDomain.com','Ms.','Steven Kramer','06-12-1974','(0114) 4960841','darmstrong@hotmail.com','436 Kim Gardens Apt. 042\nSouth Mark, MI 32684','7154','North James','Minnesota','Burkina Faso','2014','00:20:22','http://www.padilla-castillo.org/','as','2021-05-20 13:20:33');
/*!40000 ALTER TABLE `clients` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sales`
--

DROP TABLE IF EXISTS `sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sales` (
  `row_id` bigint DEFAULT NULL,
  `email` varchar(130) DEFAULT NULL,
  `item_id` varchar(130) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `volume` int DEFAULT NULL,
  `price` decimal(10,0) DEFAULT NULL,
  `sales` decimal(10,0) DEFAULT NULL,
  `creation_timestamp` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Email Id,Item Id,Name,Volume,Price,Sales';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sales`
--

LOCK TABLES `sales` WRITE;
/*!40000 ALTER TABLE `sales` DISABLE KEYS */;
INSERT INTO `sales` VALUES (0,'Clayton.Miller@testDomain.com','178','Public-key clear-thinking conglomeration',9,43,383,'2021-05-20 13:20:33'),(1,'Clayton.Miller@testDomain.com','469','Pre-emptive analyzing matrix',3,138,413,'2021-05-20 13:20:33'),(2,'Clayton.Miller@testDomain.com','280','Pre-emptive eco-centric Local Area Network',16,333,5324,'2021-05-20 13:20:33'),(3,'Clayton.Miller@testDomain.com','478','Grass-roots maximized conglomeration',8,215,1720,'2021-05-20 13:20:33'),(4,'Clayton.Miller@testDomain.com','177','Focused eco-centric info-mediaries',14,217,3037,'2021-05-20 13:20:33'),(5,'Clayton.Miller@testDomain.com','137','Virtual asymmetric standardization',7,225,1578,'2021-05-20 13:20:33'),(6,'Clayton.Miller@testDomain.com','405','Synergized value-added matrix',7,347,2429,'2021-05-20 13:20:33'),(7,'Clayton.Miller@testDomain.com','479','Multi-channeled bifurcated projection',6,234,1403,'2021-05-20 13:20:33'),(8,'Clayton.Miller@testDomain.com','327','Synchronized even-keeled firmware',6,245,1468,'2021-05-20 13:20:33'),(9,'Thomas.Wilson@testDomain.com','128','Digitized heuristic website',20,112,2239,'2021-05-20 13:20:33'),(10,'Thomas.Wilson@testDomain.com','270','Centralized impactful definition',20,154,3079,'2021-05-20 13:20:33'),(11,'Thomas.Wilson@testDomain.com','197','Optional upward-trending data-warehouse',1,216,216,'2021-05-20 13:20:33'),(12,'Thomas.Wilson@testDomain.com','388','Self-enabling interactive budgetary management',8,181,1446,'2021-05-20 13:20:33'),(13,'Thomas.Wilson@testDomain.com','260','Robust mobile function',3,236,707,'2021-05-20 13:20:33'),(14,'Thomas.Wilson@testDomain.com','248','Configurable mobile secured line',20,384,7677,'2021-05-20 13:20:33'),(15,'Thomas.Wilson@testDomain.com','306','Extended multimedia framework',7,27,192,'2021-05-20 13:20:33'),(16,'Dr..Monica@testDomain.com','281','Re-engineered discrete solution',4,176,702,'2021-05-20 13:20:33'),(17,'Dr..Monica@testDomain.com','258','Persevering uniform moderator',12,77,927,'2021-05-20 13:20:33'),(18,'Dr..Monica@testDomain.com','435','Business-focused bi-directional hierarchy',13,254,3303,'2021-05-20 13:20:33'),(19,'Dr..Monica@testDomain.com','253','Function-based optimal workforce',5,49,245,'2021-05-20 13:20:33'),(20,'Dr..Monica@testDomain.com','490','Upgradable 4thgeneration complexity',3,320,961,'2021-05-20 13:20:33'),(21,'Dr..Monica@testDomain.com','261','Upgradable client-driven conglomeration',13,71,919,'2021-05-20 13:20:33'),(22,'Dr..Monica@testDomain.com','473','Decentralized upward-trending moderator',12,203,2440,'2021-05-20 13:20:33'),(23,'Dr..Monica@testDomain.com','124','Organic 5thgeneration system engine',12,64,768,'2021-05-20 13:20:33'),(24,'Todd.Luna@testDomain.com','129','Universal multimedia Local Area Network',5,70,352,'2021-05-20 13:20:33'),(25,'Jeremy.Thomas@testDomain.com','263','Visionary web-enabled middleware',10,30,303,'2021-05-20 13:20:33'),(26,'Jeremy.Thomas@testDomain.com','119','Persistent transitional structure',17,84,1430,'2021-05-20 13:20:33'),(27,'Reginald.Morris@testDomain.com','365','Quality-focused empowering contingency',9,233,2100,'2021-05-20 13:20:33'),(28,'Reginald.Morris@testDomain.com','294','Assimilated value-added instruction set',10,233,2330,'2021-05-20 13:20:33'),(29,'Reginald.Morris@testDomain.com','160','Re-contextualized motivating support',18,343,6179,'2021-05-20 13:20:33'),(30,'Reginald.Morris@testDomain.com','188','Triple-buffered scalable budgetary management',4,365,1459,'2021-05-20 13:20:33'),(31,'James.Brooks@testDomain.com','357','Visionary even-keeled complexity',2,39,78,'2021-05-20 13:20:33'),(32,'James.Brooks@testDomain.com','463','Virtual attitude-oriented secured line',19,172,3277,'2021-05-20 13:20:33'),(33,'James.Brooks@testDomain.com','333','Front-line intermediate product',16,238,3807,'2021-05-20 13:20:33'),(34,'James.Brooks@testDomain.com','121','Innovative scalable open system',11,69,754,'2021-05-20 13:20:33'),(0,'Clayton.Miller@testDomain.com','178','Public-key clear-thinking conglomeration',9,43,383,'2021-05-20 13:20:33'),(1,'Clayton.Miller@testDomain.com','469','Pre-emptive analyzing matrix',3,138,413,'2021-05-20 13:20:33'),(2,'Clayton.Miller@testDomain.com','280','Pre-emptive eco-centric Local Area Network',16,333,5324,'2021-05-20 13:20:33'),(3,'Clayton.Miller@testDomain.com','478','Grass-roots maximized conglomeration',8,215,1720,'2021-05-20 13:20:33'),(4,'Clayton.Miller@testDomain.com','177','Focused eco-centric info-mediaries',14,217,3037,'2021-05-20 13:20:33'),(5,'Clayton.Miller@testDomain.com','137','Virtual asymmetric standardization',7,225,1578,'2021-05-20 13:20:33'),(6,'Clayton.Miller@testDomain.com','405','Synergized value-added matrix',7,347,2429,'2021-05-20 13:20:33'),(7,'Clayton.Miller@testDomain.com','479','Multi-channeled bifurcated projection',6,234,1403,'2021-05-20 13:20:33'),(8,'Clayton.Miller@testDomain.com','327','Synchronized even-keeled firmware',6,245,1468,'2021-05-20 13:20:33'),(9,'Thomas.Wilson@testDomain.com','128','Digitized heuristic website',20,112,2239,'2021-05-20 13:20:33'),(10,'Thomas.Wilson@testDomain.com','270','Centralized impactful definition',20,154,3079,'2021-05-20 13:20:33'),(11,'Thomas.Wilson@testDomain.com','197','Optional upward-trending data-warehouse',1,216,216,'2021-05-20 13:20:33'),(12,'Thomas.Wilson@testDomain.com','388','Self-enabling interactive budgetary management',8,181,1446,'2021-05-20 13:20:33'),(13,'Thomas.Wilson@testDomain.com','260','Robust mobile function',3,236,707,'2021-05-20 13:20:33'),(14,'Thomas.Wilson@testDomain.com','248','Configurable mobile secured line',20,384,7677,'2021-05-20 13:20:33'),(15,'Thomas.Wilson@testDomain.com','306','Extended multimedia framework',7,27,192,'2021-05-20 13:20:33'),(16,'Dr..Monica@testDomain.com','281','Re-engineered discrete solution',4,176,702,'2021-05-20 13:20:33'),(17,'Dr..Monica@testDomain.com','258','Persevering uniform moderator',12,77,927,'2021-05-20 13:20:33'),(18,'Dr..Monica@testDomain.com','435','Business-focused bi-directional hierarchy',13,254,3303,'2021-05-20 13:20:33'),(19,'Dr..Monica@testDomain.com','253','Function-based optimal workforce',5,49,245,'2021-05-20 13:20:33'),(20,'Dr..Monica@testDomain.com','490','Upgradable 4thgeneration complexity',3,320,961,'2021-05-20 13:20:33'),(21,'Dr..Monica@testDomain.com','261','Upgradable client-driven conglomeration',13,71,919,'2021-05-20 13:20:33'),(22,'Dr..Monica@testDomain.com','473','Decentralized upward-trending moderator',12,203,2440,'2021-05-20 13:20:33'),(23,'Dr..Monica@testDomain.com','124','Organic 5thgeneration system engine',12,64,768,'2021-05-20 13:20:33'),(24,'Todd.Luna@testDomain.com','129','Universal multimedia Local Area Network',5,70,352,'2021-05-20 13:20:33'),(25,'Jeremy.Thomas@testDomain.com','263','Visionary web-enabled middleware',10,30,303,'2021-05-20 13:20:33'),(26,'Jeremy.Thomas@testDomain.com','119','Persistent transitional structure',17,84,1430,'2021-05-20 13:20:33'),(27,'Reginald.Morris@testDomain.com','365','Quality-focused empowering contingency',9,233,2100,'2021-05-20 13:20:33'),(28,'Reginald.Morris@testDomain.com','294','Assimilated value-added instruction set',10,233,2330,'2021-05-20 13:20:33'),(29,'Reginald.Morris@testDomain.com','160','Re-contextualized motivating support',18,343,6179,'2021-05-20 13:20:33'),(30,'Reginald.Morris@testDomain.com','188','Triple-buffered scalable budgetary management',4,365,1459,'2021-05-20 13:20:33'),(31,'James.Brooks@testDomain.com','357','Visionary even-keeled complexity',2,39,78,'2021-05-20 13:20:33'),(32,'James.Brooks@testDomain.com','463','Virtual attitude-oriented secured line',19,172,3277,'2021-05-20 13:20:33'),(33,'James.Brooks@testDomain.com','333','Front-line intermediate product',16,238,3807,'2021-05-20 13:20:33'),(34,'James.Brooks@testDomain.com','121','Innovative scalable open system',11,69,754,'2021-05-20 13:20:33'),(0,'Clayton.Miller@testDomain.com','178','Public-key clear-thinking conglomeration',9,43,383,'2021-05-20 13:20:33'),(1,'Clayton.Miller@testDomain.com','469','Pre-emptive analyzing matrix',3,138,413,'2021-05-20 13:20:33'),(2,'Clayton.Miller@testDomain.com','280','Pre-emptive eco-centric Local Area Network',16,333,5324,'2021-05-20 13:20:33'),(3,'Clayton.Miller@testDomain.com','478','Grass-roots maximized conglomeration',8,215,1720,'2021-05-20 13:20:33'),(4,'Clayton.Miller@testDomain.com','177','Focused eco-centric info-mediaries',14,217,3037,'2021-05-20 13:20:33'),(5,'Clayton.Miller@testDomain.com','137','Virtual asymmetric standardization',7,225,1578,'2021-05-20 13:20:33'),(6,'Clayton.Miller@testDomain.com','405','Synergized value-added matrix',7,347,2429,'2021-05-20 13:20:33'),(7,'Clayton.Miller@testDomain.com','479','Multi-channeled bifurcated projection',6,234,1403,'2021-05-20 13:20:33'),(8,'Clayton.Miller@testDomain.com','327','Synchronized even-keeled firmware',6,245,1468,'2021-05-20 13:20:33'),(9,'Thomas.Wilson@testDomain.com','128','Digitized heuristic website',20,112,2239,'2021-05-20 13:20:33'),(10,'Thomas.Wilson@testDomain.com','270','Centralized impactful definition',20,154,3079,'2021-05-20 13:20:33'),(11,'Thomas.Wilson@testDomain.com','197','Optional upward-trending data-warehouse',1,216,216,'2021-05-20 13:20:33'),(12,'Thomas.Wilson@testDomain.com','388','Self-enabling interactive budgetary management',8,181,1446,'2021-05-20 13:20:33'),(13,'Thomas.Wilson@testDomain.com','260','Robust mobile function',3,236,707,'2021-05-20 13:20:33'),(14,'Thomas.Wilson@testDomain.com','248','Configurable mobile secured line',20,384,7677,'2021-05-20 13:20:33'),(15,'Thomas.Wilson@testDomain.com','306','Extended multimedia framework',7,27,192,'2021-05-20 13:20:33'),(16,'Dr..Monica@testDomain.com','281','Re-engineered discrete solution',4,176,702,'2021-05-20 13:20:33'),(17,'Dr..Monica@testDomain.com','258','Persevering uniform moderator',12,77,927,'2021-05-20 13:20:33'),(18,'Dr..Monica@testDomain.com','435','Business-focused bi-directional hierarchy',13,254,3303,'2021-05-20 13:20:33'),(19,'Dr..Monica@testDomain.com','253','Function-based optimal workforce',5,49,245,'2021-05-20 13:20:33'),(20,'Dr..Monica@testDomain.com','490','Upgradable 4thgeneration complexity',3,320,961,'2021-05-20 13:20:33'),(21,'Dr..Monica@testDomain.com','261','Upgradable client-driven conglomeration',13,71,919,'2021-05-20 13:20:33'),(22,'Dr..Monica@testDomain.com','473','Decentralized upward-trending moderator',12,203,2440,'2021-05-20 13:20:33'),(23,'Dr..Monica@testDomain.com','124','Organic 5thgeneration system engine',12,64,768,'2021-05-20 13:20:33'),(24,'Todd.Luna@testDomain.com','129','Universal multimedia Local Area Network',5,70,352,'2021-05-20 13:20:33'),(25,'Jeremy.Thomas@testDomain.com','263','Visionary web-enabled middleware',10,30,303,'2021-05-20 13:20:33'),(26,'Jeremy.Thomas@testDomain.com','119','Persistent transitional structure',17,84,1430,'2021-05-20 13:20:33'),(27,'Reginald.Morris@testDomain.com','365','Quality-focused empowering contingency',9,233,2100,'2021-05-20 13:20:33'),(28,'Reginald.Morris@testDomain.com','294','Assimilated value-added instruction set',10,233,2330,'2021-05-20 13:20:33'),(29,'Reginald.Morris@testDomain.com','160','Re-contextualized motivating support',18,343,6179,'2021-05-20 13:20:33'),(30,'Reginald.Morris@testDomain.com','188','Triple-buffered scalable budgetary management',4,365,1459,'2021-05-20 13:20:33'),(31,'James.Brooks@testDomain.com','357','Visionary even-keeled complexity',2,39,78,'2021-05-20 13:20:33'),(32,'James.Brooks@testDomain.com','463','Virtual attitude-oriented secured line',19,172,3277,'2021-05-20 13:20:33'),(33,'James.Brooks@testDomain.com','333','Front-line intermediate product',16,238,3807,'2021-05-20 13:20:33'),(34,'James.Brooks@testDomain.com','121','Innovative scalable open system',11,69,754,'2021-05-20 13:20:33');
/*!40000 ALTER TABLE `sales` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2021-03-20 21:27:24
