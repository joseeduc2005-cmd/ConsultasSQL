// src/domain/User.ts

export type UserRole = 'admin' | 'user';

export interface IUser {
  id: string;
  username: string;
  password: string;
  role: UserRole;
  createdAt: Date;
}

export class User implements IUser {
  id: string;
  username: string;
  password: string;
  role: UserRole;
  createdAt: Date;

  constructor(
    id: string,
    username: string,
    password: string,
    role: UserRole = 'user',
    createdAt: Date = new Date()
  ) {
    this.id = id;
    this.username = username;
    this.password = password;
    this.role = role;
    this.createdAt = createdAt;
  }

  isAdmin(): boolean {
    return this.role === 'admin';
  }
}
