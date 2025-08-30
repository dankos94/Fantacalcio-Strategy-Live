import dayjs from 'dayjs';
import { ISquadra } from 'app/shared/model/squadra.model';

export interface IWatchList {
  id?: number;
  date?: dayjs.Dayjs | null;
  version?: string | null;
  squadra?: ISquadra | null;
}

export const defaultValue: Readonly<IWatchList> = {};
