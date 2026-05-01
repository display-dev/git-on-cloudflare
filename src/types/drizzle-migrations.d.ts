type DurableSqliteMigrationConfig = {
  journal: {
    entries: {
      idx: number;
      when: number;
      tag: string;
      breakpoints: boolean;
    }[];
  };
  migrations: Record<string, string>;
};

declare module "*/drizzle/repo-do/migrations.js" {
  const migrations: DurableSqliteMigrationConfig;
  export default migrations;
}
