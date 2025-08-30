package it.costantino.astaapp.web.rest;

import it.costantino.astaapp.repository.RosterRepository;
import it.costantino.astaapp.service.RosterService;
import it.costantino.astaapp.service.dto.RosterDTO;
import it.costantino.astaapp.web.rest.errors.BadRequestAlertException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link it.costantino.astaapp.domain.Roster}.
 */
@RestController
@RequestMapping("/api/rosters")
public class RosterResource {

    private static final Logger LOG = LoggerFactory.getLogger(RosterResource.class);

    private static final String ENTITY_NAME = "roster";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final RosterService rosterService;

    private final RosterRepository rosterRepository;

    public RosterResource(RosterService rosterService, RosterRepository rosterRepository) {
        this.rosterService = rosterService;
        this.rosterRepository = rosterRepository;
    }

    /**
     * {@code POST  /rosters} : Create a new roster.
     *
     * @param rosterDTO the rosterDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new rosterDTO, or with status {@code 400 (Bad Request)} if the roster has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<RosterDTO> createRoster(@Valid @RequestBody RosterDTO rosterDTO) throws URISyntaxException {
        LOG.debug("REST request to save Roster : {}", rosterDTO);
        if (rosterDTO.getId() != null) {
            throw new BadRequestAlertException("A new roster cannot already have an ID", ENTITY_NAME, "idexists");
        }
        rosterDTO = rosterService.save(rosterDTO);
        return ResponseEntity.created(new URI("/api/rosters/" + rosterDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, false, ENTITY_NAME, rosterDTO.getId().toString()))
            .body(rosterDTO);
    }

    /**
     * {@code PUT  /rosters/:id} : Updates an existing roster.
     *
     * @param id the id of the rosterDTO to save.
     * @param rosterDTO the rosterDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated rosterDTO,
     * or with status {@code 400 (Bad Request)} if the rosterDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the rosterDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<RosterDTO> updateRoster(
        @PathVariable(value = "id", required = false) final Long id,
        @Valid @RequestBody RosterDTO rosterDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update Roster : {}, {}", id, rosterDTO);
        if (rosterDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, rosterDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!rosterRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        rosterDTO = rosterService.update(rosterDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, rosterDTO.getId().toString()))
            .body(rosterDTO);
    }

    /**
     * {@code PATCH  /rosters/:id} : Partial updates given fields of an existing roster, field will ignore if it is null
     *
     * @param id the id of the rosterDTO to save.
     * @param rosterDTO the rosterDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated rosterDTO,
     * or with status {@code 400 (Bad Request)} if the rosterDTO is not valid,
     * or with status {@code 404 (Not Found)} if the rosterDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the rosterDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<RosterDTO> partialUpdateRoster(
        @PathVariable(value = "id", required = false) final Long id,
        @NotNull @RequestBody RosterDTO rosterDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update Roster partially : {}, {}", id, rosterDTO);
        if (rosterDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, rosterDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!rosterRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<RosterDTO> result = rosterService.partialUpdate(rosterDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, false, ENTITY_NAME, rosterDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /rosters} : get all the rosters.
     *
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of rosters in body.
     */
    @GetMapping("")
    public List<RosterDTO> getAllRosters() {
        LOG.debug("REST request to get all Rosters");
        return rosterService.findAll();
    }

    /**
     * {@code GET  /rosters/:id} : get the "id" roster.
     *
     * @param id the id of the rosterDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the rosterDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<RosterDTO> getRoster(@PathVariable("id") Long id) {
        LOG.debug("REST request to get Roster : {}", id);
        Optional<RosterDTO> rosterDTO = rosterService.findOne(id);
        return ResponseUtil.wrapOrNotFound(rosterDTO);
    }

    /**
     * {@code DELETE  /rosters/:id} : delete the "id" roster.
     *
     * @param id the id of the rosterDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteRoster(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete Roster : {}", id);
        rosterService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, false, ENTITY_NAME, id.toString()))
            .build();
    }
}
