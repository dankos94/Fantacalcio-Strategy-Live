import { IStagione } from 'app/shared/model/stagione.model';

export interface ILega {
  id?: number;
  nome?: string;
  budget?: number;
  stagione?: IStagione | null;
}

export const defaultValue: Readonly<ILega> = {};
