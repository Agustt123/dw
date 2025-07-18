/*
 Navicat Premium Dump SQL

 Source Server         : Datawarehouse
 Source Server Type    : MariaDB
 Source Server Version : 100527 (10.5.27-MariaDB)
 Source Host           : 149.56.182.49:44349
 Source Schema         : data

 Target Server Type    : MariaDB
 Target Server Version : 100527 (10.5.27-MariaDB)
 File Encoding         : 65001

 Date: 30/06/2025 17:17:10
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for home_app
-- ----------------------------
DROP TABLE IF EXISTS `home_app`;
CREATE TABLE `home_app`  (
  `id` int(111) NOT NULL AUTO_INCREMENT,
  `dIdOwner` int(11) NOT NULL DEFAULT 0,
  `didCliente` int(11) NOT NULL DEFAULT 0,
  `didChofer` int(11) NOT NULL DEFAULT 0,
  `fecha` date NOT NULL DEFAULT '0000-00-00',
  `entregadosHoy` int(11) NOT NULL DEFAULT 0,
  `asignadosHoy` int(11) NOT NULL DEFAULT 0,
  `enCaminoHoy` int(11) NOT NULL DEFAULT 0,
  `cerradosHoy` int(11) NOT NULL DEFAULT 0,
  `pendientes` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
