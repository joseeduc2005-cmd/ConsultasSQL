// src/interfaces/validators.ts

export function validateInput(input: string, fieldName: string, minLength: number = 1, maxLength: number = 255): void {
  if (!input || typeof input !== 'string') {
    throw new Error(`${fieldName} es requerido`);
  }

  if (input.trim().length < minLength) {
    throw new Error(`${fieldName} debe tener al menos ${minLength} caracteres`);
  }

  if (input.length > maxLength) {
    throw new Error(`${fieldName} no puede exceder ${maxLength} caracteres`);
  }
}

export function validateEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}
