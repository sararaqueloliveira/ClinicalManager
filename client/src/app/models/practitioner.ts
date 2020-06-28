export class Practitioner {
  constructor(
    public id : string = null,
    public name ?: string,
    public gender ?: string,
    public city ?: string,
    public state ?: string,
    public postalCode ?: string,
    public gender_flag ?: boolean,
    public active ?: boolean
  ) {}
}
