import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...clases: ClassValue[]) {
  return twMerge(clsx(clases));
}
