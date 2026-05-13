import type { FileIconName } from "@/shared/web";
import {
  Database,
  File,
  FileCode2,
  FileDiff,
  FileImage,
  FileSpreadsheet,
  FileSymlink,
  FileText,
  Folder,
  Terminal,
  type LucideIcon,
} from "lucide-react";

type FileIconProps = {
  name: FileIconName;
  className?: string;
};

const iconByName: Record<FileIconName, LucideIcon> = {
  code: FileCode2,
  database: Database,
  diff: FileDiff,
  file: File,
  folder: Folder,
  image: FileImage,
  spreadsheet: FileSpreadsheet,
  symlink: FileSymlink,
  terminal: Terminal,
  text: FileText,
};

export function FileIcon({ name, className }: FileIconProps) {
  const Icon = iconByName[name];
  return <Icon className={className} aria-hidden="true" />;
}
