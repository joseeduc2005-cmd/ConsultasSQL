// src/application/usecases/loginUser.ts

import { User } from '../../domain/User';
import { UserRepository } from '../../infrastructure/userRepository';
import * as bcrypt from 'bcryptjs';

export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  user: {
    id: string;
    username: string;
    role: string;
  };
  token: string;
}

export class LoginUserUseCase {
  private userRepository: UserRepository;

  constructor(userRepository: UserRepository) {
    this.userRepository = userRepository;
  }

  async execute(request: LoginRequest): Promise<LoginResponse> {
    const user = await this.userRepository.findByUsername(request.username);

    if (!user) {
      throw new Error('Usuario no encontrado');
    }

    const passwordMatch = await bcrypt.compare(request.password, user.password);

    if (!passwordMatch) {
      throw new Error('Contraseña incorrecta');
    }

    // TODOs: Generar JWT (se implementa en el controlador)
    return {
      user: {
        id: user.id,
        username: user.username,
        role: user.role,
      },
      token: '', // El token se genera en el controlador
    };
  }
}
