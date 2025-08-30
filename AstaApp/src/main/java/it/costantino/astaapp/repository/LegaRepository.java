package it.costantino.astaapp.repository;

import it.costantino.astaapp.domain.Lega;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA repository for the Lega entity.
 */
@SuppressWarnings("unused")
@Repository
public interface LegaRepository extends JpaRepository<Lega, Long> {}
