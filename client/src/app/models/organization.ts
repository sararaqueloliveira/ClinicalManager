export class Organization {
  constructor(
    public id : string = null,
    public name ?: string,
    public city ?: string,
    public state ?: string,
    public postalCode ?: string,
    public phone ?: string
  ) {}
}
