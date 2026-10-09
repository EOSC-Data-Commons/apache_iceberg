{
  title: .metadata.title,
  resource_type: .metadata.resource_type.id,
  creators: [(.metadata.creators // [])[] | .person_or_org.name]
}   