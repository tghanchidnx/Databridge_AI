/*
  Warnings:

  - You are about to drop the `hierarchy_configurations` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `hierarchy_nodes` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `hierarchy_settings` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `hierarchy_templates` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `hierarchy_versions` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `smart_hierarchy_exports` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE `hierarchy_configurations` DROP FOREIGN KEY `hierarchy_configurations_project_id_fkey`;

-- DropForeignKey
ALTER TABLE `hierarchy_nodes` DROP FOREIGN KEY `hierarchy_nodes_project_id_fkey`;

-- DropForeignKey
ALTER TABLE `hierarchy_templates` DROP FOREIGN KEY `hierarchy_templates_project_id_fkey`;

-- DropForeignKey
ALTER TABLE `hierarchy_versions` DROP FOREIGN KEY `hierarchy_versions_project_id_fkey`;

-- DropForeignKey
ALTER TABLE `smart_hierarchy_exports` DROP FOREIGN KEY `smart_hierarchy_exports_project_id_fkey`;

-- DropTable
DROP TABLE `hierarchy_configurations`;

-- DropTable
DROP TABLE `hierarchy_nodes`;

-- DropTable
DROP TABLE `hierarchy_settings`;

-- DropTable
DROP TABLE `hierarchy_templates`;

-- DropTable
DROP TABLE `hierarchy_versions`;

-- DropTable
DROP TABLE `smart_hierarchy_exports`;
