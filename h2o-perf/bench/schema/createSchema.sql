CREATE TABLE `python_message` (
  `python_message_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(2) NOT NULL,
  `message` longtext NOT NULL,
  `row` longtext NOT NULL,
  PRIMARY KEY (`python_message_id`),
  UNIQUE KEY `python_message_id_UNIQUE` (`python_message_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run` (
  `test_run_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(100) NOT NULL COMMENT 'Who performed the run.',
  `test_name` varchar(100) NOT NULL,
  `product_name` varchar(100) NOT NULL,
  `component_name` varchar(100) NOT NULL COMMENT 'e.g.  glm1, glm2, gbm2, rf1, drf2, kmeans1, kmeans2, summary1, summary2',
  `dataset_name` varchar(100) NOT NULL DEFAULT '' COMMENT 'e.g. iris, covtype, etc.\n',
  `dataset_source` varchar(100) NOT NULL DEFAULT '' COMMENT 'e.g.  s3, hdfs, local, nfs, upload\n',
  `build_version` varchar(100) NOT NULL COMMENT 'e.g. 2.0.0.0\n',
  `build_branch` varchar(100) NOT NULL DEFAULT '',
  `build_date` varchar(100) NOT NULL DEFAULT '',
  `build_sha` varchar(100) NOT NULL DEFAULT '',
  `start_epoch_ms` bigint(20) NOT NULL DEFAULT '-1',
  `end_epoch_ms` bigint(20) NOT NULL DEFAULT '-1',
  `datacenter` varchar(100) NOT NULL DEFAULT '' COMMENT '0xdata, ec2',
  `instance_type` varchar(100) NOT NULL DEFAULT '' COMMENT 'mr-0x, mr-0xb; m2-xlarge, m2-2xlarge',
  `total_hosts` int(11) NOT NULL,
  `cpus_per_host` int(11) NOT NULL,
  `total_nodes` int(11) NOT NULL COMMENT 'Total worker instances across all nodes.',
  `heap_bytes_per_node` bigint(20) NOT NULL DEFAULT '-1',
  `passed` tinyint(1) NOT NULL DEFAULT '0',
  `correctness_passed` tinyint(1) NOT NULL DEFAULT '0',
  `timing_passed` tinyint(1) NOT NULL DEFAULT '0',
  `error_message` text NOT NULL,
  `contaminated` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'If the run was contaminated by some other workload, for example.',
  `contamination_message` longtext NOT NULL,
  `train_dataset_url` varchar(1000) NOT NULL DEFAULT '',
  `test_dataset_url` varchar(1000) NOT NULL DEFAULT '',
  PRIMARY KEY (`test_run_id`),
  UNIQUE KEY `test_run_id_UNIQUE` (`test_run_id`)
) ENGINE=InnoDB AUTO_INCREMENT=51 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_binomial_classification_result` (
  `test_run_id` bigint(20) NOT NULL,
  `auc` double NOT NULL DEFAULT '-1',
  `preccision` double NOT NULL DEFAULT '-1',
  `recall` double NOT NULL DEFAULT '-1',
  `error_rate` double NOT NULL DEFAULT '-1',
  `minority_error_rate` double NOT NULL DEFAULT '-1',
  PRIMARY KEY (`test_run_id`),
  UNIQUE KEY `test_run_id_UNIQUE` (`test_run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_clustering_result` (
  `test_run_clustering_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `k` int(11) NOT NULL DEFAULT '-1' COMMENT 'Number of centers.',
  `withinss` double NOT NULL DEFAULT '-1',
  PRIMARY KEY (`test_run_clustering_result_id`),
  UNIQUE KEY `test_run_clustering_result_id_UNIQUE` (`test_run_clustering_result_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_cm_result` (
  `test_run_cm_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `levels_json` longtext NOT NULL COMMENT 'A dense JSON list of all levels, starting at 0.\nLevels are named.  If you like, you can name it the string "0", "1", etc.\n\ne.g.\n[ "level0", "level1", "level2", … ]\n[ "0", "1", "2", … ]\n',
  `cm_json` text NOT NULL COMMENT 'UNDEFINED\n\nFor the ''undefined'' representation (-1), this field is empty.\n\n\nDENSE\n\nFor the ''dense'' representation (0), this field contains a full JSON matrix of actual / predicted.\n\nFor example:\n\nIf levels_json contains: ["0", "1"]\n\nAnd your test result is /* comment truncated */ /*:\n\na           predicted\nc\nt              "0"   "1"\nu      "0"   10   20\na      "1"   30   40\nl\n\nThen this field should contain:\n\n[ [10, 20], [30, 40] ]\n*/',
  `representation` tinyint(4) NOT NULL DEFAULT '-1' COMMENT '-1 means undefined\n0 means dense\n1 means sparse (currently unsupported)\n',
  PRIMARY KEY (`test_run_cm_result_id`),
  UNIQUE KEY `test_run_cm_result_id_UNIQUE` (`test_run_cm_result_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_host` (
  `test_run_host_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `ip` varchar(50) NOT NULL,
  `host_name` varchar(100) NOT NULL,
  `num_cpus` int(11) NOT NULL,
  `memory_bytes` bigint(20) NOT NULL,
  PRIMARY KEY (`test_run_host_id`),
  UNIQUE KEY `test_run_host_id_UNIQUE` (`test_run_host_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_jvm` (
  `test_run_jvm_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `ip` varchar(50) NOT NULL,
  `port` int(11) NOT NULL,
  `heap_bytes` bigint(20) NOT NULL,
  `java_launch_command` varchar(1000) NOT NULL COMMENT 'The full java command used to start h2o.\n',
  `java_dash_version` varchar(1000) NOT NULL COMMENT 'Full output of java -version.',
  `java_version` varchar(50) NOT NULL COMMENT 'e.g.  1.6.0_65\n',
  PRIMARY KEY (`test_run_jvm_id`),
  UNIQUE KEY `test_run_jvm_id_UNIQUE` (`test_run_jvm_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_model_result` (
  `test_run_id` bigint(20) NOT NULL,
  `model_json` longtext NOT NULL,
  `test_run_model_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`test_run_model_result_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_multinomial_classification_result` (
  `test_run_multinomial_classification_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `level` varchar(100) NOT NULL DEFAULT '',
  `level_actual_count` bigint(20) NOT NULL DEFAULT '-1',
  `level_predicted_correctly_count` bigint(20) NOT NULL DEFAULT '-1',
  `level_error_rate` double NOT NULL DEFAULT '-1',
  PRIMARY KEY (`test_run_multinomial_classification_result_id`),
  UNIQUE KEY `test_run_multinomial_classification_result_id_UNIQUE` (`test_run_multinomial_classification_result_id`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_parameter` (
  `test_run_parameter_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `name` varchar(100) NOT NULL,
  `value` text NOT NULL,
  PRIMARY KEY (`test_run_parameter_id`),
  UNIQUE KEY `test_run_parameter_id_UNIQUE` (`test_run_parameter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_phase_result` (
  `test_run_phase_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `phase_name` varchar(50) NOT NULL,
  `start_epoch_ms` bigint(20) NOT NULL,
  `end_epoch_ms` bigint(20) NOT NULL,
  `stdouterr` longtext,
  `passed` tinyint(1) NOT NULL DEFAULT '0',
  `correctness_passed` tinyint(1) NOT NULL DEFAULT '0',
  `timing_passed` tinyint(1) NOT NULL DEFAULT '0',
  `contaminated` tinyint(1) NOT NULL DEFAULT '0',
  `contamination_message` longtext,
  PRIMARY KEY (`test_run_phase_result_id`),
  UNIQUE KEY `test_run_phase_result_id_UNIQUE` (`test_run_phase_result_id`)
) ENGINE=InnoDB AUTO_INCREMENT=52111111112 DEFAULT CHARSET=utf8;

CREATE TABLE `test_run_regression_result` (
  `test_run_regression_result_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `test_run_id` bigint(20) NOT NULL,
  `aic` double NOT NULL DEFAULT '-1',
  `null_deviance` double NOT NULL DEFAULT '-1',
  `residual_deviance` double NOT NULL DEFAULT '-1',
  PRIMARY KEY (`test_run_regression_result_id`),
  UNIQUE KEY `test_run_regression_result_id_UNIQUE` (`test_run_regression_result_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

