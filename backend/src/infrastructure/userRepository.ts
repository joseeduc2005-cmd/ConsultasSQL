// src/infrastructure/userRepository.ts

import { User } from '../domain/User';
import { getDatabase } from './database';
import { randomUUID } from 'crypto';

export interface IUserRepository {
  findByUsername(username: string): Promise<User | null>;
  findById(id: string): Promise<User | null>;
  create(user: User): Promise<User>;
  update(user: User): Promise<User>;
  delete(id: string): Promise<boolean>;
}

export class UserRepository implements IUserRepository {
  async findByUsername(username: string): Promise<User | null> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'SELECT id, username, password, role, created_at FROM users WHERE username = $1',
        [username]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return new User(row.id, row.username, row.password, row.role, new Date(row.created_at));
    } catch (error) {
      console.error('Error en findByUsername:', error);
      throw error;
    }
  }

  async findById(id: string): Promise<User | null> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'SELECT id, username, password, role, created_at FROM users WHERE id = $1',
        [id]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return new User(row.id, row.username, row.password, row.role, new Date(row.created_at));
    } catch (error) {
      console.error('Error en findById:', error);
      throw error;
    }
  }

  async create(user: User): Promise<User> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'INSERT INTO users (id, username, password, role, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING *',
        [user.id || randomUUID(), user.username, user.password, user.role, user.createdAt]
      );

      const row = result.rows[0];
      return new User(row.id, row.username, row.password, row.role, new Date(row.created_at));
    } catch (error) {
      console.error('Error en create:', error);
      throw error;
    }
  }

  async update(user: User): Promise<User> {
    const db = getDatabase();
    try {
      const result = await db.query(
        'UPDATE users SET username = $1, password = $2, role = $3 WHERE id = $4 RETURNING *',
        [user.username, user.password, user.role, user.id]
      );

      const row = result.rows[0];
      return new User(row.id, row.username, row.password, row.role, new Date(row.created_at));
    } catch (error) {
      console.error('Error en update:', error);
      throw error;
    }
  }

  async delete(id: string): Promise<boolean> {
    const db = getDatabase();
    try {
      const result = await db.query('DELETE FROM users WHERE id = $1', [id]);
      return (result.rowCount ?? 0) > 0;
    } catch (error) {
      console.error('Error en delete:', error);
      throw error;
    }
  }
}
