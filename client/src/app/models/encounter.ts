export class Encounter {
  constructor(
    public id : string = null,
    public status ?: boolean,
    public type ?: string,
    public patient ?: string,
    public practitioner ?: string,
    public start ?: string,
    public end ?: string
  ) {}
}
