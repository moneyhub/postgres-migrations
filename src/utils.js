const quoteIdent = (str) => {
  return `"${str.replace(/"/g, "\"\"")}"`
}

module.exports = {
  quoteIdent,
}
