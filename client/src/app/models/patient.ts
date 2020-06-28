export class Patient {
  constructor(
    public id : string = null,
    public name ?: string,
    public gender ?: string,
    public birthDate ?: string,
    public city ?: string,
    public state ?: string,
    public postalCode ?: string,
    public gender_flag ?: boolean,
    public socialSecurity ?: string,
    public phone ?: string,
    public maritalStatus ?: string,
    public communication ?: string
  ) {}
}


