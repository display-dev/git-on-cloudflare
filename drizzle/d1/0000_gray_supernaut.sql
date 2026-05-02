CREATE TABLE `users` (
	`id` text PRIMARY KEY NOT NULL,
	`tessera_sub` text NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `users_tessera_sub_unique` ON `users` (`tessera_sub`);--> statement-breakpoint
CREATE TABLE `namespaces` (
	`id` text PRIMARY KEY NOT NULL,
	`slug` text NOT NULL,
	`created_by` text NOT NULL,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`created_by`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE restrict
);
--> statement-breakpoint
CREATE UNIQUE INDEX `namespaces_slug_unique` ON `namespaces` (`slug`);--> statement-breakpoint
CREATE TABLE `namespace_memberships` (
	`namespace_id` text NOT NULL,
	`user_id` text NOT NULL,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`namespace_id`, `user_id`),
	FOREIGN KEY (`namespace_id`) REFERENCES `namespaces`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_namespace_memberships_user_ns` ON `namespace_memberships` (`user_id`,`namespace_id`);--> statement-breakpoint
CREATE TABLE `repositories` (
	`id` text PRIMARY KEY NOT NULL,
	`namespace_id` text NOT NULL,
	`created_by` text NOT NULL,
	`slug` text NOT NULL,
	`do_name` text NOT NULL,
	`visibility` text NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`namespace_id`) REFERENCES `namespaces`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`created_by`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE restrict,
	CONSTRAINT "chk_repositories_visibility" CHECK("visibility" IN ('public','private'))
);
--> statement-breakpoint
CREATE UNIQUE INDEX `uq_repositories_namespace_slug` ON `repositories` (`namespace_id`,`slug`);--> statement-breakpoint
CREATE UNIQUE INDEX `uq_repositories_do_name` ON `repositories` (`do_name`);--> statement-breakpoint
CREATE INDEX `idx_repositories_namespace_updated` ON `repositories` (`namespace_id`,"updated_at" desc,`slug`);--> statement-breakpoint
CREATE TABLE `personal_access_tokens` (
	`id` text PRIMARY KEY NOT NULL,
	`user_id` text NOT NULL,
	`name` text NOT NULL,
	`prefix` text NOT NULL,
	`hash` text NOT NULL,
	`created_at` integer NOT NULL,
	`expires_at` integer,
	`revoked_at` integer,
	`last_used_at` integer,
	FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `personal_access_tokens_prefix_unique` ON `personal_access_tokens` (`prefix`);--> statement-breakpoint
CREATE INDEX `idx_pats_user_created` ON `personal_access_tokens` (`user_id`,"created_at" desc);--> statement-breakpoint
CREATE TABLE `pat_namespace_grants` (
	`pat_id` text NOT NULL,
	`namespace_id` text NOT NULL,
	`level` text NOT NULL,
	PRIMARY KEY(`pat_id`, `namespace_id`),
	FOREIGN KEY (`pat_id`) REFERENCES `personal_access_tokens`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`namespace_id`) REFERENCES `namespaces`(`id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_pat_namespace_grants_level" CHECK("level" IN ('pull','push'))
);
--> statement-breakpoint
CREATE INDEX `idx_pat_namespace_grants_namespace` ON `pat_namespace_grants` (`namespace_id`);--> statement-breakpoint
CREATE TABLE `pat_repo_grants` (
	`pat_id` text NOT NULL,
	`repo_id` text NOT NULL,
	`level` text NOT NULL,
	PRIMARY KEY(`pat_id`, `repo_id`),
	FOREIGN KEY (`pat_id`) REFERENCES `personal_access_tokens`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`repo_id`) REFERENCES `repositories`(`id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_pat_repo_grants_level" CHECK("level" IN ('pull','push'))
);
--> statement-breakpoint
CREATE INDEX `idx_pat_repo_grants_repo` ON `pat_repo_grants` (`repo_id`);